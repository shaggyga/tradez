#!/usr/bin/env python3
"""Continuously promote proven live-paper predictor cells into practice_007.

The source ledger contains dense, overlapping forecasts.  Promotion therefore
uses one executable-return average per non-overlapping horizon block rather
than treating every snapshot as an independent trial.  Eligibility is exact
to model, artifact hash, instrument, input timeframe, and outcome horizon.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import statistics
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parent
STATE_ROOT = ROOT / "data" / "oanda_training_manager" / "state"
DEFAULT_SOURCE = STATE_ROOT / "model_gap_live_forecasts_v1.sqlite"
DEFAULT_DATABASE = STATE_ROOT / "model_predictor_auto_promotion_v1.sqlite"
DEFAULT_STATE = STATE_ROOT / "model_predictor_auto_promotion_v1.json"
ACCOUNT_SCOPE = "practice_007_only"
STATE_SCHEMA_VERSION = 1


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return default
    return output if math.isfinite(output) else default


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def state_checksum(payload: dict[str, Any]) -> str:
    canonical = dict(payload)
    canonical.pop("state_sha256", None)
    encoded = json.dumps(
        canonical,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def verify_state_checksum(payload: dict[str, Any]) -> bool:
    expected = str(payload.get("state_sha256") or "")
    return bool(expected and expected == state_checksum(payload))


def cell_id(
    model_id: str,
    artifact_sha256: str,
    instrument: str,
    input_timeframe: str,
    horizon_sec: int,
) -> str:
    identity = "|".join(
        (
            model_id,
            artifact_sha256.lower(),
            instrument,
            input_timeframe.upper(),
            str(int(horizon_sec)),
        )
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]


def _session_name(epoch: float) -> str:
    hour = datetime.fromtimestamp(epoch, timezone.utc).hour
    if hour < 7:
        return "asia"
    if hour < 13:
        return "london"
    if hour < 17:
        return "london_new_york_overlap"
    if hour < 22:
        return "new_york"
    return "rollover"


def _metrics(values: Iterable[float]) -> dict[str, Any]:
    rows = [float(value) for value in values if math.isfinite(float(value))]
    if not rows:
        return {
            "n": 0,
            "avg": 0.0,
            "median": 0.0,
            "win_rate": 0.0,
            "lower_confidence": -999.0,
        }
    average = statistics.fmean(rows)
    standard_error = (
        statistics.stdev(rows) / math.sqrt(len(rows)) if len(rows) > 1 else math.inf
    )
    lower = average - 1.96 * standard_error if math.isfinite(standard_error) else -999.0
    return {
        "n": len(rows),
        "avg": round(average, 6),
        "median": round(statistics.median(rows), 6),
        "win_rate": round(100.0 * sum(value > 0.0 for value in rows) / len(rows), 6),
        "lower_confidence": round(lower, 6),
    }


@dataclass(frozen=True)
class PredictorPromotionThresholds:
    min_samples: int = 120
    min_independent_blocks: int = 12
    min_holdout_blocks: int = 4
    min_average_net_pips: float = 0.10
    min_median_net_pips: float = 0.0
    min_block_win_rate: float = 52.0
    min_lower_confidence_pips: float = 0.0
    min_direction_accuracy: float = 0.52
    min_sessions: int = 2
    max_average_entry_spread_pips: float = 5.0
    max_retained_blocks: int = 120
    allow_provisional_practice: bool = True
    provisional_min_samples: int = 240
    provisional_min_independent_blocks: int = 4
    provisional_min_holdout_blocks: int = 2
    provisional_min_direction_accuracy: float = 0.60
    provisional_max_average_entry_spread_pips: float = 3.0


def evaluate_cell_blocks(
    rows: list[dict[str, Any]],
    thresholds: PredictorPromotionThresholds,
) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda row: int(row["block_id"]))
    block_net = [
        _finite(row["sum_net_pips"]) / max(1, int(row["sample_count"]))
        for row in ordered
    ]
    split = (
        max(1, min(len(ordered) - 1, int(len(ordered) * 0.70)))
        if len(ordered) > 1
        else len(ordered)
    )
    training = _metrics(block_net[:split])
    holdout = _metrics(block_net[split:])
    total_samples = sum(int(row["sample_count"]) for row in ordered)
    direction_accuracy = (
        sum(_finite(row["direction_correct"]) for row in ordered)
        / max(1, total_samples)
    )
    average_entry_spread = (
        sum(_finite(row["sum_entry_spread_pips"]) for row in ordered)
        / max(1, total_samples)
    )
    sessions = {
        _session_name(_finite(row["max_generated_epoch"])) for row in ordered
    }
    holdout_values = block_net[split:]
    stability = []
    if holdout_values:
        for index in range(min(3, len(holdout_values))):
            start = index * len(holdout_values) // min(3, len(holdout_values))
            end = (index + 1) * len(holdout_values) // min(3, len(holdout_values))
            stability.append(statistics.fmean(holdout_values[start:end]))
    reasons: list[str] = []
    if total_samples < thresholds.min_samples:
        reasons.append("minimum_samples")
    if len(ordered) < thresholds.min_independent_blocks:
        reasons.append("minimum_independent_blocks")
    if len(holdout_values) < thresholds.min_holdout_blocks:
        reasons.append("minimum_holdout_blocks")
    if training["avg"] < thresholds.min_average_net_pips:
        reasons.append("training_net_edge")
    if holdout["avg"] < thresholds.min_average_net_pips:
        reasons.append("holdout_net_edge")
    if holdout["median"] < thresholds.min_median_net_pips:
        reasons.append("holdout_median")
    if holdout["win_rate"] < thresholds.min_block_win_rate:
        reasons.append("holdout_block_win_rate")
    if holdout["lower_confidence"] < thresholds.min_lower_confidence_pips:
        reasons.append("holdout_confidence")
    if direction_accuracy < thresholds.min_direction_accuracy:
        reasons.append("direction_accuracy")
    if len(sessions) < thresholds.min_sessions:
        reasons.append("session_breadth")
    if average_entry_spread > thresholds.max_average_entry_spread_pips:
        reasons.append("spread_cap")
    if len(stability) < 3 or any(value <= 0.0 for value in stability):
        reasons.append("time_block_stability")
    strict_eligible = not reasons
    provisional_reasons: list[str] = []
    if not thresholds.allow_provisional_practice:
        provisional_reasons.append("provisional_disabled")
    if total_samples < thresholds.provisional_min_samples:
        provisional_reasons.append("provisional_minimum_samples")
    if len(ordered) < thresholds.provisional_min_independent_blocks:
        provisional_reasons.append("provisional_minimum_independent_blocks")
    if len(holdout_values) < thresholds.provisional_min_holdout_blocks:
        provisional_reasons.append("provisional_minimum_holdout_blocks")
    if training["avg"] < thresholds.min_average_net_pips:
        provisional_reasons.append("provisional_training_net_edge")
    if holdout["avg"] < thresholds.min_average_net_pips:
        provisional_reasons.append("provisional_holdout_net_edge")
    if holdout["median"] < thresholds.min_median_net_pips:
        provisional_reasons.append("provisional_holdout_median")
    if holdout["win_rate"] < thresholds.min_block_win_rate:
        provisional_reasons.append("provisional_holdout_block_win_rate")
    if holdout["lower_confidence"] < thresholds.min_lower_confidence_pips:
        provisional_reasons.append("provisional_holdout_confidence")
    if direction_accuracy < thresholds.provisional_min_direction_accuracy:
        provisional_reasons.append("provisional_direction_accuracy")
    if len(sessions) < thresholds.min_sessions:
        provisional_reasons.append("provisional_session_breadth")
    if (
        average_entry_spread
        > thresholds.provisional_max_average_entry_spread_pips
    ):
        provisional_reasons.append("provisional_spread_cap")
    if not holdout_values or any(value <= 0.0 for value in holdout_values):
        provisional_reasons.append("provisional_holdout_stability")
    provisional_eligible = bool(not strict_eligible and not provisional_reasons)
    eligible = bool(strict_eligible or provisional_eligible)
    conservative_edge = min(
        _finite(holdout["avg"]),
        _finite(holdout["median"]),
        _finite(holdout["lower_confidence"]),
    )
    return {
        "eligible": eligible,
        "strict_eligible": strict_eligible,
        "provisional_eligible": provisional_eligible,
        "promotion_tier": (
            "strict"
            if strict_eligible
            else "practice_provisional_temporal_breadth"
            if provisional_eligible
            else "shadow"
        ),
        "blocked_by": [] if eligible else reasons,
        "strict_blocked_by": reasons,
        "provisional_blocked_by": provisional_reasons,
        "sample_count": total_samples,
        "independent_blocks": len(ordered),
        "holdout_blocks": len(holdout_values),
        "split": "oldest 70% non-overlapping blocks / newest 30% holdout",
        "raw": _metrics(block_net),
        "training": training,
        "holdout": holdout,
        "direction_accuracy": round(direction_accuracy, 6),
        "average_entry_spread_pips": round(average_entry_spread, 6),
        "session_count": len(sessions),
        "sessions": sorted(sessions),
        "stability_chunk_average_net_pips": [round(value, 6) for value in stability],
        "conservative_expected_net_pips": round(max(0.0, conservative_edge), 6),
        "score": round(_finite(holdout["lower_confidence"]), 6),
    }


class PredictorPromotionStore:
    """Incrementally collapse the dense source ledger into horizon blocks."""

    def __init__(self, source_path: Path, database_path: Path) -> None:
        self.source_path = Path(source_path)
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.database_path, timeout=60.0)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA busy_timeout=60000")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cell_blocks (
                model_id TEXT NOT NULL,
                artifact_sha256 TEXT NOT NULL,
                instrument TEXT NOT NULL,
                input_timeframe TEXT NOT NULL,
                horizon_sec INTEGER NOT NULL,
                block_id INTEGER NOT NULL,
                sample_count INTEGER NOT NULL,
                sum_net_pips REAL NOT NULL,
                sum_net_pips_sq REAL NOT NULL,
                executable_wins INTEGER NOT NULL,
                direction_correct INTEGER NOT NULL,
                sum_brier REAL NOT NULL,
                sum_entry_spread_pips REAL NOT NULL,
                sum_exit_spread_pips REAL NOT NULL,
                min_generated_epoch REAL NOT NULL,
                max_generated_epoch REAL NOT NULL,
                PRIMARY KEY(
                    model_id, artifact_sha256, instrument,
                    input_timeframe, horizon_sec, block_id
                )
            ) WITHOUT ROWID;
            """
        )
        self.connection.commit()

    def _metadata_int(self, key: str) -> int:
        row = self.connection.execute(
            "SELECT value FROM metadata WHERE key = ?", (key,)
        ).fetchone()
        return int(row[0]) if row else 0

    def _set_metadata(self, key: str, value: Any) -> None:
        self.connection.execute(
            """
            INSERT INTO metadata(key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, str(value)),
        )

    def reset(self) -> None:
        self.connection.execute("DELETE FROM cell_blocks")
        self.connection.execute("DELETE FROM metadata")
        self.connection.commit()

    def ingest(self, *, chunk_rows: int, max_chunks: int) -> dict[str, Any]:
        if not self.source_path.is_file():
            return {"status": "source_missing", "ingested_rows": 0}
        source = sqlite3.connect(
            f"file:{self.source_path.resolve().as_posix()}?mode=ro",
            uri=True,
            timeout=60.0,
        )
        source.execute("PRAGMA busy_timeout=60000")
        started = time.monotonic()
        try:
            outcome_columns = {
                str(row[1])
                for row in source.execute("PRAGMA table_info(outcomes)").fetchall()
            }
            if "artifact_sha256" in outcome_columns:
                artifact_expression = "o.artifact_sha256"
                artifact_join = ""
            else:
                artifact_expression = "p.artifact_sha256"
                artifact_join = """
                    JOIN predictions p
                      ON p.candidate_id = o.candidate_id
                     AND p.horizon_sec = o.horizon_sec
                """
            source_max = int(
                source.execute("SELECT COALESCE(MAX(rowid), 0) FROM outcomes").fetchone()[0]
            )
            cursor = self._metadata_int("source_outcome_rowid")
            if source_max < cursor:
                self.reset()
                cursor = 0
            initial_cursor = cursor
            aggregate_rows = 0
            chunks = 0
            while cursor < source_max and chunks < max(1, int(max_chunks)):
                upper = min(source_max, cursor + max(1, int(chunk_rows)))
                rows = source.execute(
                    f"""
                    SELECT
                        o.model_id,
                        {artifact_expression},
                        o.instrument,
                        UPPER(o.input_timeframe),
                        o.horizon_sec,
                        CAST(
                            o.generated_epoch /
                            CASE WHEN o.horizon_sec < 900 THEN 900 ELSE o.horizon_sec END
                            AS INTEGER
                        ) AS block_id,
                        COUNT(*),
                        SUM(o.executable_net_pips),
                        SUM(o.executable_net_pips * o.executable_net_pips),
                        SUM(o.executable_profitable),
                        SUM(o.direction_correct),
                        SUM(o.brier),
                        SUM(o.entry_spread_pips),
                        SUM(o.exit_spread_pips),
                        MIN(o.generated_epoch),
                        MAX(o.generated_epoch)
                    FROM outcomes o
                    {artifact_join}
                    WHERE o.rowid > ? AND o.rowid <= ? AND o.status = 'matured'
                    GROUP BY
                        o.model_id, {artifact_expression}, o.instrument,
                        UPPER(o.input_timeframe), o.horizon_sec, block_id
                    """,
                    (cursor, upper),
                ).fetchall()
                self.connection.executemany(
                    """
                    INSERT INTO cell_blocks(
                        model_id, artifact_sha256, instrument, input_timeframe,
                        horizon_sec, block_id, sample_count, sum_net_pips,
                        sum_net_pips_sq, executable_wins, direction_correct,
                        sum_brier, sum_entry_spread_pips, sum_exit_spread_pips,
                        min_generated_epoch, max_generated_epoch
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(
                        model_id, artifact_sha256, instrument,
                        input_timeframe, horizon_sec, block_id
                    ) DO UPDATE SET
                        sample_count=sample_count + excluded.sample_count,
                        sum_net_pips=sum_net_pips + excluded.sum_net_pips,
                        sum_net_pips_sq=sum_net_pips_sq + excluded.sum_net_pips_sq,
                        executable_wins=executable_wins + excluded.executable_wins,
                        direction_correct=direction_correct + excluded.direction_correct,
                        sum_brier=sum_brier + excluded.sum_brier,
                        sum_entry_spread_pips=(
                            sum_entry_spread_pips + excluded.sum_entry_spread_pips
                        ),
                        sum_exit_spread_pips=(
                            sum_exit_spread_pips + excluded.sum_exit_spread_pips
                        ),
                        min_generated_epoch=MIN(
                            min_generated_epoch, excluded.min_generated_epoch
                        ),
                        max_generated_epoch=MAX(
                            max_generated_epoch, excluded.max_generated_epoch
                        )
                    """,
                    rows,
                )
                cursor = upper
                self._set_metadata("source_outcome_rowid", cursor)
                self._set_metadata("source_path", str(self.source_path.resolve()))
                self.connection.commit()
                aggregate_rows += len(rows)
                chunks += 1
            return {
                "status": "caught_up" if cursor >= source_max else "backfilling",
                "source_max_rowid": source_max,
                "cursor_rowid": cursor,
                "ingested_rows": cursor - initial_cursor,
                "aggregate_rows": aggregate_rows,
                "chunks": chunks,
                "elapsed_sec": round(time.monotonic() - started, 3),
            }
        finally:
            source.close()

    def prune(self, now_epoch: float, max_blocks: int) -> int:
        before = self.connection.total_changes
        self.connection.execute(
            """
            DELETE FROM cell_blocks
            WHERE block_id < CAST(
                ? / CASE WHEN horizon_sec < 900 THEN 900 ELSE horizon_sec END
                AS INTEGER
            ) - ?
            """,
            (float(now_epoch), max(1, int(max_blocks))),
        )
        self.connection.commit()
        return self.connection.total_changes - before

    def evaluate(
        self,
        thresholds: PredictorPromotionThresholds,
        prior_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        columns = (
            "model_id",
            "artifact_sha256",
            "instrument",
            "input_timeframe",
            "horizon_sec",
            "block_id",
            "sample_count",
            "sum_net_pips",
            "sum_net_pips_sq",
            "executable_wins",
            "direction_correct",
            "sum_brier",
            "sum_entry_spread_pips",
            "sum_exit_spread_pips",
            "min_generated_epoch",
            "max_generated_epoch",
        )
        cursor = self.connection.execute(
            """
            SELECT model_id, artifact_sha256, instrument, input_timeframe,
                   horizon_sec, block_id, sample_count, sum_net_pips,
                   sum_net_pips_sq, executable_wins, direction_correct,
                   sum_brier, sum_entry_spread_pips, sum_exit_spread_pips,
                   min_generated_epoch, max_generated_epoch
            FROM cell_blocks
            ORDER BY model_id, artifact_sha256, instrument,
                     input_timeframe, horizon_sec, block_id
            """
        )
        previous = {
            str(row.get("cell_id")): row
            for row in (prior_state or {}).get("eligible_cells") or []
            if isinstance(row, dict)
        }
        eligible: list[dict[str, Any]] = []
        top: list[dict[str, Any]] = []
        blocked = Counter()
        evaluated = 0
        current_key: tuple[Any, ...] | None = None
        group: list[dict[str, Any]] = []

        def consume(key: tuple[Any, ...] | None, rows: list[dict[str, Any]]) -> None:
            nonlocal evaluated
            if key is None or not rows:
                return
            evaluated += 1
            model_id, artifact_sha, instrument, timeframe, horizon = key
            evidence = evaluate_cell_blocks(rows, thresholds)
            if not artifact_sha:
                evidence["eligible"] = False
                evidence["blocked_by"] = sorted(
                    {*evidence["blocked_by"], "artifact_hash_missing"}
                )
            identity = cell_id(
                str(model_id),
                str(artifact_sha),
                str(instrument),
                str(timeframe),
                int(horizon),
            )
            row = {
                "cell_id": identity,
                "model_id": str(model_id),
                "artifact_sha256": str(artifact_sha),
                "instrument": str(instrument),
                "input_timeframe": str(timeframe),
                "horizon_sec": int(horizon),
                **evidence,
            }
            for reason in evidence["blocked_by"]:
                blocked[reason] += 1
            top.append(row)
            if evidence["eligible"]:
                prior = previous.get(identity) or {}
                row["promoted_utc"] = str(prior.get("promoted_utc") or utc_now())
                eligible.append(row)

        for values in cursor:
            row = dict(zip(columns, values))
            key = tuple(row[name] for name in columns[:5])
            if current_key is not None and key != current_key:
                consume(current_key, group)
                group = []
            current_key = key
            group.append(row)
        consume(current_key, group)

        top.sort(
            key=lambda row: (
                bool(row["eligible"]),
                _finite(row["score"], -999.0),
                _finite(row["holdout"]["avg"], -999.0),
                int(row["independent_blocks"]),
            ),
            reverse=True,
        )
        eligible.sort(
            key=lambda row: (
                _finite(row["score"], -999.0),
                _finite(row["holdout"]["avg"], -999.0),
            ),
            reverse=True,
        )
        return {
            "evaluated_cell_count": evaluated,
            "eligible_cells": eligible,
            "top_evidence": top[:250],
            "blocked_reason_counts": dict(sorted(blocked.items())),
        }

    def block_count(self) -> int:
        return int(self.connection.execute("SELECT COUNT(*) FROM cell_blocks").fetchone()[0])

    def close(self) -> None:
        self.connection.close()


def build_state(
    store: PredictorPromotionStore,
    thresholds: PredictorPromotionThresholds,
    ingest: dict[str, Any],
    prior_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    evaluation = store.evaluate(thresholds, prior_state)
    eligible = evaluation["eligible_cells"]
    payload: dict[str, Any] = {
        "schema_version": STATE_SCHEMA_VERSION,
        "generated_utc": utc_now(),
        "generated_epoch": time.time(),
        "status": "active",
        "account_scope": ACCOUNT_SCOPE,
        "real_account_authorized": False,
        "expires_after_sec": 1800,
        "source": {
            "ledger": str(store.source_path.resolve()),
            "aggregation_database": str(store.database_path.resolve()),
            "ingest": ingest,
            "retained_block_count": store.block_count(),
        },
        "thresholds": asdict(thresholds),
        "eligible_cell_count": len(eligible),
        "strict_eligible_cell_count": sum(
            bool(row.get("strict_eligible")) for row in eligible
        ),
        "provisional_eligible_cell_count": sum(
            bool(row.get("provisional_eligible")) for row in eligible
        ),
        "eligible_model_count": len({row["model_id"] for row in eligible}),
        "eligible_cells": eligible,
        "top_evidence": evaluation["top_evidence"],
        "evaluated_cell_count": evaluation["evaluated_cell_count"],
        "blocked_reason_counts": evaluation["blocked_reason_counts"],
        "contract": {
            "scope_is_exact_model_artifact_pair_timeframe_horizon": True,
            "outcomes_are_observed_executable_bid_ask_pips": True,
            "dense_snapshots_are_collapsed_into_non_overlapping_blocks": True,
            "selection_is_chronological_70_30": True,
            "artifact_hash_match_required_at_inference": True,
            "stale_policy_fails_closed": True,
            "demotion_is_automatic_when_gates_fail": True,
            "provisional_cells_are_practice_only": True,
            "provisional_cells_require_positive_costed_train_and_holdout": True,
            "real_accounts_remain_read_only": True,
        },
    }
    payload["state_sha256"] = state_checksum(payload)
    return payload


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-ledger", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--chunk-rows", type=int, default=250000)
    parser.add_argument("--max-chunks", type=int, default=20)
    parser.add_argument("--interval-sec", type=float, default=300.0)
    parser.add_argument("--min-samples", type=int, default=120)
    parser.add_argument("--min-independent-blocks", type=int, default=12)
    parser.add_argument("--min-holdout-blocks", type=int, default=4)
    parser.add_argument("--min-average-net-pips", type=float, default=0.10)
    parser.add_argument("--min-median-net-pips", type=float, default=0.0)
    parser.add_argument("--min-block-win-rate", type=float, default=52.0)
    parser.add_argument("--min-lower-confidence-pips", type=float, default=0.0)
    parser.add_argument("--min-direction-accuracy", type=float, default=0.52)
    parser.add_argument("--min-sessions", type=int, default=2)
    parser.add_argument("--max-average-entry-spread-pips", type=float, default=5.0)
    parser.add_argument("--max-retained-blocks", type=int, default=120)
    parser.add_argument(
        "--allow-provisional-practice",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--provisional-min-samples", type=int, default=240)
    parser.add_argument("--provisional-min-independent-blocks", type=int, default=4)
    parser.add_argument("--provisional-min-holdout-blocks", type=int, default=2)
    parser.add_argument("--provisional-min-direction-accuracy", type=float, default=0.60)
    parser.add_argument(
        "--provisional-max-average-entry-spread-pips",
        type=float,
        default=3.0,
    )
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    if min(
        args.chunk_rows,
        args.max_chunks,
        args.min_samples,
        args.min_independent_blocks,
        args.min_holdout_blocks,
        args.min_sessions,
        args.max_retained_blocks,
        args.provisional_min_samples,
        args.provisional_min_independent_blocks,
        args.provisional_min_holdout_blocks,
    ) <= 0 or args.interval_sec < 0.0:
        raise SystemExit("promotion counts must be positive and interval non-negative")
    return args


def run(args: argparse.Namespace) -> int:
    thresholds = PredictorPromotionThresholds(
        min_samples=args.min_samples,
        min_independent_blocks=args.min_independent_blocks,
        min_holdout_blocks=args.min_holdout_blocks,
        min_average_net_pips=args.min_average_net_pips,
        min_median_net_pips=args.min_median_net_pips,
        min_block_win_rate=args.min_block_win_rate,
        min_lower_confidence_pips=args.min_lower_confidence_pips,
        min_direction_accuracy=args.min_direction_accuracy,
        min_sessions=args.min_sessions,
        max_average_entry_spread_pips=args.max_average_entry_spread_pips,
        max_retained_blocks=args.max_retained_blocks,
        allow_provisional_practice=args.allow_provisional_practice,
        provisional_min_samples=args.provisional_min_samples,
        provisional_min_independent_blocks=args.provisional_min_independent_blocks,
        provisional_min_holdout_blocks=args.provisional_min_holdout_blocks,
        provisional_min_direction_accuracy=args.provisional_min_direction_accuracy,
        provisional_max_average_entry_spread_pips=(
            args.provisional_max_average_entry_spread_pips
        ),
    )
    store = PredictorPromotionStore(args.source_ledger, args.database)
    try:
        while True:
            started = time.monotonic()
            ingest = store.ingest(chunk_rows=args.chunk_rows, max_chunks=args.max_chunks)
            store.prune(time.time(), thresholds.max_retained_blocks)
            prior = read_json(args.state)
            state = build_state(store, thresholds, ingest, prior)
            atomic_json(args.state, state)
            print(
                json.dumps(
                    {
                        "status": ingest.get("status"),
                        "eligible_cells": state["eligible_cell_count"],
                        "evaluated_cells": state["evaluated_cell_count"],
                        "blocks": state["source"]["retained_block_count"],
                        "elapsed_sec": round(time.monotonic() - started, 3),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            if args.once or args.interval_sec <= 0.0:
                return 0
            time.sleep(args.interval_sec)
    finally:
        store.close()


def main(argv: Iterable[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
