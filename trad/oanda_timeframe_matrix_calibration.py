#!/usr/bin/env python3
"""Legacy row-order calibration diagnostics for the timeframe equation matrix.

The retained outcomes lack original forecast-issue and committed label-availability
certificates. Scoring before each row's own update does not make this replay a
causal out-of-sample evaluation. Numeric diagnostics and bins remain available;
new publications cannot certify readiness. The separate availability-aware V2
evaluator does not import or relabel these historical rows.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


BIN_COUNT = 10
PRIOR_STRENGTH = 20.0
WARMUP_ROWS = 40


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def clamp_probability(value: Any) -> float:
    return max(0.001, min(0.999, finite(value, 0.5)))


def maturity_block_count(blocks: set[str], horizon_sec: int) -> int:
    """Count non-overlapping horizon-sized blocks from retained UTC hours."""

    width_sec = max(3600, int(horizon_sec or 0))
    maturity_blocks: set[int] = set()
    for value in blocks:
        try:
            observed = datetime.fromisoformat(f"{value}:00:00+00:00")
            maturity_blocks.add(int(observed.timestamp()) // width_sec)
        except (TypeError, ValueError):
            # Preserve conservative observability for legacy/malformed values.
            maturity_blocks.add(hash(str(value)))
    return len(maturity_blocks)


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str),
        encoding="utf-8",
    )
    os.replace(temporary, path)


@dataclass
class SurfaceStats:
    scope: str
    surface_key: str
    lane_id: str
    instrument: str
    horizon_sec: int
    total: int = 0
    oos: int = 0
    raw_brier_sum: float = 0.0
    calibrated_brier_sum: float = 0.0
    raw_correct: int = 0
    calibrated_correct: int = 0
    raw_net_sum: float = 0.0
    calibrated_net_sum: float = 0.0
    calibrated_net_sq_sum: float = 0.0
    calibrated_net_positive: int = 0
    bins: list[list[int]] = field(
        default_factory=lambda: [[0, 0] for _ in range(BIN_COUNT)]
    )
    blocks: set[str] = field(default_factory=set)

    @classmethod
    def from_database(cls, row: sqlite3.Row) -> "SurfaceStats":
        bins = json.loads(str(row["bins_json"]) or "[]")
        normalized_bins = [
            [int(values[0]), int(values[1])]
            for values in bins[:BIN_COUNT]
            if isinstance(values, list) and len(values) >= 2
        ]
        normalized_bins.extend([[0, 0] for _ in range(BIN_COUNT - len(normalized_bins))])
        return cls(
            scope=str(row["scope"]),
            surface_key=str(row["surface_key"]),
            lane_id=str(row["lane_id"]),
            instrument=str(row["instrument"]),
            horizon_sec=int(row["horizon_sec"]),
            total=int(row["total"]),
            oos=int(row["oos"]),
            raw_brier_sum=finite(row["raw_brier_sum"]),
            calibrated_brier_sum=finite(row["calibrated_brier_sum"]),
            raw_correct=int(row["raw_correct"]),
            calibrated_correct=int(row["calibrated_correct"]),
            raw_net_sum=finite(row["raw_net_sum"]),
            calibrated_net_sum=finite(row["calibrated_net_sum"]),
            calibrated_net_sq_sum=finite(row["calibrated_net_sq_sum"]),
            calibrated_net_positive=int(row["calibrated_net_positive"]),
            bins=normalized_bins,
            blocks=set(json.loads(str(row["blocks_json"]) or "[]")),
        )

    def calibrated_probability(self, raw_probability_up: float) -> float:
        raw = clamp_probability(raw_probability_up)
        index = min(BIN_COUNT - 1, int(raw * BIN_COUNT))
        count, up_count = self.bins[index]
        center = (index + 0.5) / BIN_COUNT
        return (up_count + PRIOR_STRENGTH * center) / (
            count + PRIOR_STRENGTH
        )

    def observe(
        self,
        *,
        raw_probability_up: float,
        actual_up: float,
        long_net_pips: float,
        short_net_pips: float,
        observed_utc: str,
    ) -> None:
        raw = clamp_probability(raw_probability_up)
        calibrated = self.calibrated_probability(raw)
        if self.total >= WARMUP_ROWS:
            self.oos += 1
            self.raw_brier_sum += (raw - actual_up) ** 2
            self.calibrated_brier_sum += (calibrated - actual_up) ** 2
            raw_up = raw >= 0.5
            calibrated_up = calibrated >= 0.5
            actual_is_up = actual_up >= 0.5
            self.raw_correct += int(raw_up == actual_is_up)
            self.calibrated_correct += int(calibrated_up == actual_is_up)
            raw_net = long_net_pips if raw_up else short_net_pips
            calibrated_net = long_net_pips if calibrated_up else short_net_pips
            self.raw_net_sum += raw_net
            self.calibrated_net_sum += calibrated_net
            self.calibrated_net_sq_sum += calibrated_net * calibrated_net
            self.calibrated_net_positive += int(calibrated_net > 0.0)
            if observed_utc:
                self.blocks.add(observed_utc[:13])
        index = min(BIN_COUNT - 1, int(raw * BIN_COUNT))
        self.bins[index][0] += 1
        self.bins[index][1] += int(actual_up >= 0.5)
        self.total += 1

    def state(self) -> dict[str, Any]:
        oos = max(0, self.oos)
        independent_blocks = maturity_block_count(self.blocks, self.horizon_sec)
        raw_brier = self.raw_brier_sum / oos if oos else None
        calibrated_brier = self.calibrated_brier_sum / oos if oos else None
        raw_accuracy = self.raw_correct / oos if oos else None
        calibrated_accuracy = self.calibrated_correct / oos if oos else None
        average_net = self.calibrated_net_sum / oos if oos else None
        win_rate = self.calibrated_net_positive / oos if oos else None
        lower_confidence = None
        if oos and average_net is not None:
            variance = max(
                0.0,
                self.calibrated_net_sq_sum / oos - average_net * average_net,
            )
            lower_confidence = average_net - 1.64 * math.sqrt(variance / oos)
        diagnostic_thresholds_met = bool(
            self.total >= 120
            and oos >= 80
            and independent_blocks >= 8
            and calibrated_brier is not None
            and raw_brier is not None
            and calibrated_accuracy is not None
            and average_net is not None
            and lower_confidence is not None
            and calibrated_brier <= 0.255
            and calibrated_brier <= raw_brier + 0.005
            and calibrated_accuracy >= 0.51
            and average_net > 0.03
            and lower_confidence > 0.0
        )
        bins = []
        for index, (count, up_count) in enumerate(self.bins):
            center = (index + 0.5) / BIN_COUNT
            posterior = (up_count + PRIOR_STRENGTH * center) / (
                count + PRIOR_STRENGTH
            )
            bins.append(
                {
                    "index": index,
                    "low": round(index / BIN_COUNT, 3),
                    "high": round((index + 1) / BIN_COUNT, 3),
                    "n": count,
                    "up_rate": None if not count else round(up_count / count, 6),
                    "posterior_up": round(posterior, 6),
                }
            )
        return {
            "scope": self.scope,
            "surface_key": self.surface_key,
            "lane_id": self.lane_id,
            "instrument": self.instrument,
            "horizon_sec": self.horizon_sec,
            "n": self.total,
            "oos_n": oos,
            "observed_hour_blocks": len(self.blocks),
            "independent_blocks": independent_blocks,
            "independence_block_sec": max(3600, int(self.horizon_sec)),
            "raw_brier": None if raw_brier is None else round(raw_brier, 6),
            "calibrated_brier": (
                None if calibrated_brier is None else round(calibrated_brier, 6)
            ),
            "raw_accuracy": None if raw_accuracy is None else round(raw_accuracy, 6),
            "calibrated_accuracy": (
                None if calibrated_accuracy is None else round(calibrated_accuracy, 6)
            ),
            "raw_average_net_pips": (
                None if not oos else round(self.raw_net_sum / oos, 6)
            ),
            "calibrated_average_net_pips": (
                None if average_net is None else round(average_net, 6)
            ),
            "calibrated_win_rate": None if win_rate is None else round(win_rate, 6),
            "lower_confidence_net_pips": (
                None if lower_confidence is None else round(lower_confidence, 6)
            ),
            "diagnostic_thresholds_met": diagnostic_thresholds_met,
            "validation_ready": False,
            "proof_eligible": False,
            "evaluation_kind": "legacy_row_order_replay",
            "issue_time_availability_verified": False,
            "blocked_by": ["original_issue_and_committed_label_availability_missing"],
            "status": "diagnostic_replay",
            "account_eligible": False,
            "bins": bins,
        }


class TimeframeMatrixCalibrator:
    """Process each outcome once, retaining uncertified legacy replay diagnostics."""

    def __init__(
        self,
        source_database: Path,
        calibration_database: Path,
        state_path: Path,
        *,
        batch_size: int = 50000,
    ) -> None:
        self.source_database = Path(source_database)
        self.calibration_database = Path(calibration_database)
        self.state_path = Path(state_path)
        self.batch_size = max(100, int(batch_size))
        self.calibration_database.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.calibration_database, timeout=120.0)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS calibration_meta (
                source_path TEXT PRIMARY KEY,
                last_row_id INTEGER NOT NULL,
                updated_utc TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS calibration_surfaces (
                scope TEXT NOT NULL,
                surface_key TEXT NOT NULL,
                lane_id TEXT NOT NULL,
                instrument TEXT NOT NULL,
                horizon_sec INTEGER NOT NULL,
                total INTEGER NOT NULL,
                oos INTEGER NOT NULL,
                raw_brier_sum REAL NOT NULL,
                calibrated_brier_sum REAL NOT NULL,
                raw_correct INTEGER NOT NULL,
                calibrated_correct INTEGER NOT NULL,
                raw_net_sum REAL NOT NULL,
                calibrated_net_sum REAL NOT NULL,
                calibrated_net_sq_sum REAL NOT NULL,
                calibrated_net_positive INTEGER NOT NULL,
                bins_json TEXT NOT NULL,
                blocks_json TEXT NOT NULL,
                updated_utc TEXT NOT NULL,
                PRIMARY KEY(scope, surface_key)
            );
            """
        )
        self.connection.commit()
        self.surfaces = {
            (stats.scope, stats.surface_key): stats
            for row in self.connection.execute(
                "SELECT * FROM calibration_surfaces"
            )
            for stats in [SurfaceStats.from_database(row)]
        }

    def _source_key(self) -> str:
        return str(self.source_database.resolve())

    def _cursor(self) -> int:
        row = self.connection.execute(
            "SELECT last_row_id FROM calibration_meta WHERE source_path = ?",
            (self._source_key(),),
        ).fetchone()
        return 0 if row is None else int(row[0])

    def _surface(
        self,
        scope: str,
        lane_id: str,
        instrument: str,
        horizon_sec: int,
    ) -> SurfaceStats:
        surface_key = (
            f"{lane_id}|{horizon_sec}"
            if scope == "global"
            else f"{instrument}|{lane_id}|{horizon_sec}"
        )
        key = (scope, surface_key)
        stats = self.surfaces.get(key)
        if stats is None:
            stats = SurfaceStats(
                scope=scope,
                surface_key=surface_key,
                lane_id=lane_id,
                instrument="" if scope == "global" else instrument,
                horizon_sec=horizon_sec,
            )
            self.surfaces[key] = stats
        return stats

    def _persist(self, touched: set[tuple[str, str]], cursor: int) -> None:
        stamp = utc_now()
        payload = []
        for key in touched:
            stats = self.surfaces[key]
            payload.append(
                (
                    stats.scope,
                    stats.surface_key,
                    stats.lane_id,
                    stats.instrument,
                    stats.horizon_sec,
                    stats.total,
                    stats.oos,
                    stats.raw_brier_sum,
                    stats.calibrated_brier_sum,
                    stats.raw_correct,
                    stats.calibrated_correct,
                    stats.raw_net_sum,
                    stats.calibrated_net_sum,
                    stats.calibrated_net_sq_sum,
                    stats.calibrated_net_positive,
                    json.dumps(stats.bins, separators=(",", ":")),
                    json.dumps(sorted(stats.blocks), separators=(",", ":")),
                    stamp,
                )
            )
        with self.connection:
            self.connection.executemany(
                """
                INSERT INTO calibration_surfaces VALUES (
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                )
                ON CONFLICT(scope, surface_key) DO UPDATE SET
                    lane_id=excluded.lane_id,
                    instrument=excluded.instrument,
                    horizon_sec=excluded.horizon_sec,
                    total=excluded.total,
                    oos=excluded.oos,
                    raw_brier_sum=excluded.raw_brier_sum,
                    calibrated_brier_sum=excluded.calibrated_brier_sum,
                    raw_correct=excluded.raw_correct,
                    calibrated_correct=excluded.calibrated_correct,
                    raw_net_sum=excluded.raw_net_sum,
                    calibrated_net_sum=excluded.calibrated_net_sum,
                    calibrated_net_sq_sum=excluded.calibrated_net_sq_sum,
                    calibrated_net_positive=excluded.calibrated_net_positive,
                    bins_json=excluded.bins_json,
                    blocks_json=excluded.blocks_json,
                    updated_utc=excluded.updated_utc
                """,
                payload,
            )
            self.connection.execute(
                """
                INSERT INTO calibration_meta(source_path, last_row_id, updated_utc)
                VALUES (?, ?, ?)
                ON CONFLICT(source_path) DO UPDATE SET
                    last_row_id=excluded.last_row_id,
                    updated_utc=excluded.updated_utc
                """,
                (self._source_key(), cursor, stamp),
            )

    @staticmethod
    def _decode_row(row: sqlite3.Row) -> dict[str, Any] | None:
        if str(row["family"]) != "timeframe_equation_matrix":
            return None
        try:
            diagnostics = json.loads(str(row["diagnostics_json"]) or "{}")
        except json.JSONDecodeError:
            return None
        prediction = diagnostics.get("timeframe_prediction")
        if not isinstance(prediction, dict):
            return None
        pip = finite(row["pip"])
        if pip <= 0.0:
            return None
        entry_bid = finite(row["entry_bid"])
        entry_ask = finite(row["entry_ask"])
        exit_bid = finite(row["exit_bid"])
        exit_ask = finite(row["exit_ask"])
        if min(entry_bid, entry_ask, exit_bid, exit_ask) <= 0.0:
            return None
        raw_probability = clamp_probability(
            prediction.get("raw_probability_up", prediction.get("probability_up"))
        )
        entry_mid = (entry_bid + entry_ask) / 2.0
        exit_mid = (exit_bid + exit_ask) / 2.0
        return {
            "lane_id": str(row["lane_id"]),
            "instrument": str(row["instrument"]),
            "horizon_sec": int(row["horizon_sec"]),
            "observed_utc": str(row["observed_utc"]),
            "raw_probability_up": raw_probability,
            "actual_up": 1.0 if exit_mid > entry_mid else 0.0,
            "long_net_pips": (exit_bid - entry_ask) / pip,
            "short_net_pips": (entry_bid - exit_ask) / pip,
        }

    def run_once(self) -> dict[str, Any]:
        processed_rows = 0
        calibrated_rows = 0
        cursor = self._cursor()
        if self.source_database.is_file():
            source = sqlite3.connect(
                f"file:{self.source_database.as_posix()}?mode=ro",
                uri=True,
                timeout=120.0,
            )
            source.row_factory = sqlite3.Row
            source.execute("PRAGMA busy_timeout=120000")
            while True:
                rows = source.execute(
                    """
                    SELECT row_id, observed_utc, lane_id, family, input_timeframe,
                           instrument, horizon_sec, entry_bid, entry_ask,
                           exit_bid, exit_ask, pip, diagnostics_json
                    FROM outcomes
                    WHERE row_id > ?
                    ORDER BY row_id
                    LIMIT ?
                    """,
                    (cursor, self.batch_size),
                ).fetchall()
                if not rows:
                    break
                touched: set[tuple[str, str]] = set()
                for row in rows:
                    cursor = max(cursor, int(row["row_id"]))
                    processed_rows += 1
                    decoded = self._decode_row(row)
                    if decoded is None:
                        continue
                    calibrated_rows += 1
                    for scope in ("global", "pair"):
                        stats = self._surface(
                            scope,
                            decoded["lane_id"],
                            decoded["instrument"],
                            decoded["horizon_sec"],
                        )
                        stats.observe(
                            raw_probability_up=decoded["raw_probability_up"],
                            actual_up=decoded["actual_up"],
                            long_net_pips=decoded["long_net_pips"],
                            short_net_pips=decoded["short_net_pips"],
                            observed_utc=decoded["observed_utc"],
                        )
                        touched.add((stats.scope, stats.surface_key))
                self._persist(touched, cursor)
            source.close()
        state = self.build_state(
            processed_rows=processed_rows,
            calibrated_rows=calibrated_rows,
            cursor=cursor,
        )
        atomic_json(self.state_path, state)
        return state

    def build_state(
        self,
        *,
        processed_rows: int,
        calibrated_rows: int,
        cursor: int,
    ) -> dict[str, Any]:
        states = [stats.state() for stats in self.surfaces.values()]
        global_surfaces = {
            row["surface_key"]: row
            for row in states
            if row["scope"] == "global"
        }
        pair_surfaces = {
            row["surface_key"]: row
            for row in states
            if row["scope"] == "pair" and int(row["n"]) >= 20
        }
        top = sorted(
            states,
            key=lambda row: (
                bool(row["diagnostic_thresholds_met"]),
                finite(row.get("lower_confidence_net_pips"), -999.0),
                finite(row.get("calibrated_average_net_pips"), -999.0),
                int(row["n"]),
            ),
            reverse=True,
        )[:100]
        return {
            "schema_version": 1,
            "generated_utc": utc_now(),
            "source_database": str(self.source_database.resolve()),
            "calibration_database": str(self.calibration_database.resolve()),
            "last_source_row_id": cursor,
            "processed_rows_this_run": processed_rows,
            "calibrated_rows_this_run": calibrated_rows,
            "surface_count": len(states),
            "ready_surface_count": sum(
                int(bool(row["validation_ready"])) for row in states
            ),
            "diagnostic_thresholds_met_surface_count": sum(
                int(bool(row["diagnostic_thresholds_met"])) for row in states
            ),
            "thresholds": {
                "warmup_rows": WARMUP_ROWS,
                "minimum_total_rows": 120,
                "minimum_oos_rows": 80,
                "minimum_independent_hour_blocks_legacy": 8,
                "minimum_independent_maturity_blocks": 8,
                "maximum_calibrated_brier": 0.255,
                "minimum_calibrated_accuracy": 0.51,
                "minimum_average_executable_net_pips": 0.03,
                "minimum_lower_confidence_net_pips": 0.0,
                "prior_strength": PRIOR_STRENGTH,
            },
            "global_surfaces": global_surfaces,
            "pair_surfaces": pair_surfaces,
            "top_surfaces": top,
            "account_eligible": False,
            "proof_eligible": False,
            "evaluation_kind": "legacy_row_order_replay",
            "issue_time_availability_verified": False,
            "status": "diagnostic_replay",
        }

    def close(self) -> None:
        self.connection.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-database", type=Path, required=True)
    parser.add_argument("--calibration-database", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=50000)
    parser.add_argument("--interval-sec", type=float, default=0.0)
    args = parser.parse_args(argv)
    if args.batch_size <= 0 or args.interval_sec < 0.0:
        raise SystemExit("batch size must be positive and interval must be non-negative")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    calibrator = TimeframeMatrixCalibrator(
        args.source_database,
        args.calibration_database,
        args.state,
        batch_size=args.batch_size,
    )
    try:
        while True:
            calibrator.run_once()
            if args.interval_sec <= 0.0:
                break
            time.sleep(args.interval_sec)
    finally:
        calibrator.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
