#!/usr/bin/env python3
"""Persist path-aware strategy outcomes and fit guarded practice exits."""

from __future__ import annotations

import json
import math
import sqlite3
import statistics
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    from oanda_pair_family_matrix import write_pair_family_matrix
    from oanda_statistical_validation import (
        independent_time_block_means,
        multiple_testing_metrics,
        purged_before_boundary,
        timestamp_epoch,
    )
except ModuleNotFoundError:  # Package imports used by the test suite.
    from trad.oanda_pair_family_matrix import write_pair_family_matrix
    from trad.oanda_statistical_validation import (
        independent_time_block_means,
        multiple_testing_metrics,
        purged_before_boundary,
        timestamp_epoch,
    )


STOP_GRID = (2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 12.0, 15.0)
TARGET_R_GRID = (0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 2.5, 3.0)
PATH_LEVELS = tuple(
    sorted({*STOP_GRID, *(stop * target_r for stop in STOP_GRID for target_r in TARGET_R_GRID)})
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def level_key(value: float) -> str:
    return f"{float(value):.3f}".rstrip("0").rstrip(".")


def _finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def simulate_exit(
    row: dict[str, Any],
    stop_pips: float,
    target_r: float,
) -> float:
    """Return a conservative executable outcome using first sampled barrier hits."""

    target_pips = stop_pips * target_r
    favorable = row.get("favorable_hits") or {}
    adverse = row.get("adverse_hits") or {}
    target_time = favorable.get(level_key(target_pips))
    stop_time = adverse.get(level_key(stop_pips))
    if stop_time is not None and (target_time is None or _finite(stop_time) <= _finite(target_time)):
        return -float(stop_pips)
    if target_time is not None:
        return float(target_pips)
    return _finite(row.get("endpoint_pips"))


def _metrics(values: Iterable[float]) -> dict[str, Any]:
    sample = [float(value) for value in values if math.isfinite(float(value))]
    if not sample:
        return {
            "n": 0,
            "avg_pips": 0.0,
            "median_pips": 0.0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "lower_confidence_pips": -999.0,
            "standard_deviation_pips": None,
            "sum_pips": 0.0,
        }
    average = statistics.fmean(sample)
    stdev = statistics.stdev(sample) if len(sample) > 1 else math.inf
    lower = average - 1.645 * stdev / math.sqrt(len(sample)) if math.isfinite(stdev) else -999.0
    gross_win = sum(max(0.0, value) for value in sample)
    gross_loss = abs(sum(min(0.0, value) for value in sample))
    profit_factor = gross_win / gross_loss if gross_loss > 1e-12 else (99.0 if gross_win > 0.0 else 0.0)
    return {
        "n": len(sample),
        "avg_pips": round(average, 4),
        "median_pips": round(statistics.median(sample), 4),
        "win_rate": round(100.0 * sum(value > 0.0 for value in sample) / len(sample), 2),
        "profit_factor": round(min(profit_factor, 99.0), 4),
        "lower_confidence_pips": round(lower, 4),
        "standard_deviation_pips": (
            None if not math.isfinite(stdev) else round(stdev, 4)
        ),
        "sum_pips": round(sum(sample), 4),
    }


def _percentile(values: Iterable[float], quantile: float) -> float:
    sample = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not sample:
        return 0.0
    position = max(0.0, min(1.0, float(quantile))) * (len(sample) - 1)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return sample[lower]
    weight = position - lower
    return sample[lower] * (1.0 - weight) + sample[upper] * weight


def _wilson_lower(successes: int, total: int, z_value: float = 1.645) -> float:
    if total <= 0:
        return 0.0
    probability = successes / total
    z_squared = z_value * z_value
    denominator = 1.0 + z_squared / total
    center = probability + z_squared / (2.0 * total)
    radius = z_value * math.sqrt(
        probability * (1.0 - probability) / total
        + z_squared / (4.0 * total * total)
    )
    return max(0.0, (center - radius) / denominator)


def _session_bucket(entry_time: str) -> str:
    try:
        parsed = datetime.fromisoformat(str(entry_time).replace("Z", "+00:00"))
        hour = parsed.astimezone(timezone.utc).hour
    except (TypeError, ValueError):
        return "unknown"
    if hour < 7:
        return "asia"
    if hour < 12:
        return "london"
    if hour < 17:
        return "london_new_york"
    if hour < 21:
        return "new_york"
    return "rollover"


def _quality_metrics(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = list(records)
    endpoint = [_finite(row.get("endpoint_pips")) for row in rows]
    endpoint_metrics = _metrics(endpoint)
    positive_count = sum(value > 0.0 for value in endpoint)
    favorable = [max(0.0, _finite(row.get("max_favorable_pips"))) for row in rows]
    adverse = [max(0.0, _finite(row.get("max_adverse_pips"))) for row in rows]
    spreads = [
        max(0.0, _finite(row.get("entry_spread_pips")))
        for row in rows
        if _finite(row.get("entry_spread_pips")) > 0.0
    ]
    first_positive = [
        _finite(row.get("first_positive_sec"))
        for row in rows
        if row.get("first_positive_sec") is not None
        and _finite(row.get("first_positive_sec")) >= 0.0
    ]
    path_samples = sum(max(0, int(_finite(row.get("path_samples")))) for row in rows)
    positive_path_samples = sum(
        max(0, int(_finite(row.get("positive_path_samples")))) for row in rows
    )
    ever_positive_count = sum(
        favorable[index] > 0.0 or endpoint[index] > 0.0
        for index in range(len(rows))
    )
    total = len(rows)
    probability = positive_count / total if total else 0.0
    median_spread = statistics.median(spreads) if spreads else 0.0
    median_favorable = statistics.median(favorable) if favorable else 0.0
    return {
        **endpoint_metrics,
        "positive_count": positive_count,
        "positive_probability": round(probability, 6),
        "positive_probability_lower": round(_wilson_lower(positive_count, total), 6),
        "calibrated_positive_probability": round(
            (positive_count + 5.0) / (total + 10.0) if total else 0.5,
            6,
        ),
        "ever_positive_rate": round(
            100.0 * ever_positive_count / total if total else 0.0,
            2,
        ),
        "average_mfe_pips": round(statistics.fmean(favorable), 4) if favorable else 0.0,
        "median_mfe_pips": round(median_favorable, 4),
        "mfe_p25_pips": round(_percentile(favorable, 0.25), 4),
        "average_mae_pips": round(statistics.fmean(adverse), 4) if adverse else 0.0,
        "median_mae_pips": round(statistics.median(adverse), 4) if adverse else 0.0,
        "mae_p75_pips": round(_percentile(adverse, 0.75), 4),
        "median_entry_spread_pips": round(median_spread, 4),
        "median_mfe_to_spread": round(
            median_favorable / median_spread if median_spread > 0.0 else 0.0,
            4,
        ),
        "time_to_positive_n": len(first_positive),
        "median_time_to_positive_sec": round(statistics.median(first_positive), 3)
        if first_positive
        else None,
        "p75_time_to_positive_sec": round(_percentile(first_positive, 0.75), 3)
        if first_positive
        else None,
        "path_samples": path_samples,
        "positive_path_samples": positive_path_samples,
        "positive_path_fraction": round(
            positive_path_samples / path_samples if path_samples else 0.0,
            6,
        ),
    }


def _fit_quality_group(
    records: list[dict[str, Any]],
    minimum_total: int,
    *,
    trial_count: int = 1,
) -> dict[str, Any]:
    records = sorted(
        records,
        key=lambda row: (str(row.get("entry_time") or ""), int(row.get("row_id") or 0)),
    )
    split = max(1, min(len(records) - 1, int(len(records) * 0.70))) if len(records) > 1 else len(records)
    training = records[:split]
    holdout = records[split:]
    training_metrics = _quality_metrics(training)
    holdout_metrics = _quality_metrics(holdout)
    overall_metrics = _quality_metrics(records)
    horizon_sec = max(
        1,
        int(_finite(records[0].get("horizon_sec"), 1.0)) if records else 1,
    )
    block_averages = independent_time_block_means(
        records,
        value_key="endpoint_pips",
        horizon_sec=horizon_sec,
    )
    holdout_blocks = independent_time_block_means(
        holdout,
        value_key="endpoint_pips",
        horizon_sec=horizon_sec,
    )
    selection_adjustment = multiple_testing_metrics(
        holdout_blocks,
        trial_count=max(1, int(trial_count)),
    )
    positive_blocks = sum(value > 0.0 for value in block_averages)
    required_holdout = max(20, int(math.ceil(minimum_total * 0.20)))
    required_independent = max(6, min(20, int(math.ceil(minimum_total / 20))))
    reasons: list[str] = []
    if len(records) < minimum_total:
        reasons.append("minimum_total_samples")
    if holdout_metrics["n"] < required_holdout:
        reasons.append("minimum_holdout_samples")
    if (
        training_metrics["avg_pips"] <= 0.0
        or training_metrics["lower_confidence_pips"] <= 0.0
    ):
        reasons.append("training_net_edge")
    if holdout_metrics["avg_pips"] <= 0.0 or holdout_metrics["median_pips"] < 0.0:
        reasons.append("holdout_net_edge")
    if holdout_metrics["lower_confidence_pips"] <= 0.0:
        reasons.append("holdout_net_confidence")
    if holdout_metrics["positive_probability_lower"] < 0.50:
        reasons.append("holdout_win_probability")
    if holdout_metrics["median_mfe_pips"] <= 0.0:
        reasons.append("holdout_favorable_excursion")
    if len(holdout_blocks) < required_independent:
        reasons.append("minimum_independent_blocks")
    if (
        _finite(
            selection_adjustment.get("selection_adjusted_lower_mean_pips"),
            -math.inf,
        )
        <= 0.0
    ):
        reasons.append("multiple_testing_net_confidence")
    if _finite(selection_adjustment.get("deflated_sharpe_probability")) < 0.90:
        reasons.append("deflated_sharpe_probability")
    if positive_blocks < max(1, int(math.ceil(0.60 * len(block_averages)))):
        reasons.append("time_block_stability")
    negative_evidence = bool(
        (
            len(holdout_blocks) >= required_independent
            and _finite(
                selection_adjustment.get("selection_adjusted_upper_mean_pips"),
                math.inf,
            )
            < 0.0
        )
        or
        (
            holdout_metrics["n"] >= 20
            and holdout_metrics["avg_pips"] < 0.0
            and holdout_metrics["positive_probability"] < 0.50
        )
        or (
            overall_metrics["n"] >= max(60, minimum_total // 2)
            and overall_metrics["avg_pips"] < 0.0
            and overall_metrics["lower_confidence_pips"] < 0.0
            and overall_metrics["positive_probability"] < 0.48
        )
    )
    calibration_source = holdout_metrics if holdout_metrics["n"] >= 20 else overall_metrics
    evidence_strength = min(1.0, len(records) / max(1.0, float(minimum_total)))
    return {
        "eligible": not reasons,
        "negative_evidence": negative_evidence,
        "blocked_by": reasons,
        "sample_count": len(records),
        "minimum_total_samples": int(minimum_total),
        "evidence_strength": round(evidence_strength, 6),
        "split": "oldest 70% fit / newest 30% calibration holdout",
        "calibrated_expected_net_pips": calibration_source["avg_pips"],
        "calibrated_lower_net_pips": calibration_source["lower_confidence_pips"],
        "calibrated_positive_probability": calibration_source[
            "calibrated_positive_probability"
        ],
        "training": training_metrics,
        "holdout": holdout_metrics,
        "overall": overall_metrics,
        "positive_time_blocks": positive_blocks,
        "time_block_count": len(block_averages),
        "independent_holdout_blocks": len(holdout_blocks),
        "minimum_independent_blocks": required_independent,
        "multiple_testing": selection_adjustment,
        "selection_adjusted_lower_net_pips": selection_adjustment.get(
            "selection_adjusted_lower_mean_pips"
        ),
        "deflated_sharpe_probability": selection_adjustment.get(
            "deflated_sharpe_probability"
        ),
        "trial_count": max(1, int(trial_count)),
    }


def _independent_value_blocks(
    records: list[dict[str, Any]],
    values: list[float],
    horizon_sec: int,
) -> list[float]:
    """Collapse aligned numeric outcomes without cloning path-heavy records."""

    buckets: dict[int, list[float]] = defaultdict(list)
    fallback: list[float] = []
    horizon = max(1, int(horizon_sec))
    for row, value in zip(records, values):
        numeric = _finite(value, math.nan)
        if not math.isfinite(numeric):
            continue
        epoch = timestamp_epoch(row.get("entry_time"))
        if epoch is None:
            fallback.append(numeric)
        else:
            buckets[int(epoch // horizon)].append(numeric)
    if buckets:
        return [statistics.fmean(buckets[key]) for key in sorted(buckets)]
    return fallback


def _fit_group(
    records: list[dict[str, Any]],
    minimum_total: int,
    *,
    horizon_sec: int | None = None,
) -> dict[str, Any]:
    records = sorted(
        records,
        key=lambda row: (str(row.get("entry_time") or ""), int(row.get("row_id") or 0)),
    )
    horizon = max(
        1,
        int(
            horizon_sec
            or (_finite(records[0].get("horizon_sec"), 1.0) if records else 1.0)
        ),
    )
    split = (
        max(1, min(len(records) - 1, int(len(records) * 0.70)))
        if len(records) > 1
        else len(records)
    )
    validation = records[split:]
    boundary = timestamp_epoch(validation[0].get("entry_time")) if validation else None
    development = records[:split]
    if boundary is not None:
        development = purged_before_boundary(development, boundary, horizon)

    fold_bounds = ((0.40, 0.50), (0.50, 0.60), (0.60, 1.00))
    inner_folds: list[tuple[list[dict[str, Any]], list[dict[str, Any]]]] = []
    for train_fraction, validation_fraction in fold_bounds:
        train_end = int(len(development) * train_fraction)
        validation_end = int(len(development) * validation_fraction)
        if train_end < 10 or validation_end - train_end < 5:
            continue
        inner_validation = development[train_end:validation_end]
        inner_boundary = timestamp_epoch(inner_validation[0].get("entry_time"))
        inner_training = development[:train_end]
        if inner_boundary is not None:
            inner_training = purged_before_boundary(
                inner_training,
                inner_boundary,
                horizon,
            )
        if inner_training and inner_validation:
            inner_folds.append((inner_training, inner_validation))
    if not inner_folds and development:
        inner_split = max(1, min(len(development) - 1, int(len(development) * 0.70)))
        inner_folds.append((development[:inner_split], development[inner_split:]))

    policy_trial_count = len(STOP_GRID) * len(TARGET_R_GRID)
    candidates: list[tuple[tuple[float, ...], float, float, dict[str, Any]]] = []
    for stop_pips in STOP_GRID:
        for target_r in TARGET_R_GRID:
            selection_records: list[dict[str, Any]] = []
            selection_values: list[float] = []
            for _, fold_validation in inner_folds:
                for row in fold_validation:
                    selection_records.append(row)
                    selection_values.append(simulate_exit(row, stop_pips, target_r))
            train_metrics = _metrics(selection_values)
            independent_values = _independent_value_blocks(
                selection_records,
                selection_values,
                horizon,
            )
            correction = multiple_testing_metrics(
                independent_values,
                trial_count=policy_trial_count,
            )
            score = (
                _finite(
                    correction.get("selection_adjusted_lower_mean_pips"),
                    -999.0,
                ),
                _finite(correction.get("deflated_sharpe_probability")),
                _finite(train_metrics["avg_pips"], -999.0),
                _finite(train_metrics["profit_factor"]),
                -stop_pips,
            )
            candidates.append(
                (
                    score,
                    stop_pips,
                    target_r,
                    {**train_metrics, "multiple_testing": correction},
                )
            )
    candidates.sort(key=lambda item: item[0], reverse=True)
    _, stop_pips, target_r, train_metrics = candidates[0]
    validation_metrics = _metrics(simulate_exit(row, stop_pips, target_r) for row in validation)
    all_values = [simulate_exit(row, stop_pips, target_r) for row in records]
    overall_metrics = _metrics(all_values)
    holdout_values = [
        simulate_exit(row, stop_pips, target_r) for row in validation
    ]
    block_averages = _independent_value_blocks(
        validation,
        holdout_values,
        horizon,
    )
    positive_blocks = sum(value > 0.0 for value in block_averages)
    required_validation = max(20, int(math.ceil(minimum_total * 0.20)))
    reasons: list[str] = []
    if len(records) < minimum_total:
        reasons.append("minimum_total_samples")
    if validation_metrics["n"] < required_validation:
        reasons.append("minimum_holdout_samples")
    selection_adjustment = train_metrics.get("multiple_testing") or {}
    if train_metrics["avg_pips"] <= 0.0:
        reasons.append("inner_walkforward_edge")
    if _finite(
        selection_adjustment.get("selection_adjusted_lower_mean_pips"),
        -math.inf,
    ) <= 0.0:
        reasons.append("inner_walkforward_multiple_testing")
    if _finite(selection_adjustment.get("deflated_sharpe_probability")) < 0.90:
        reasons.append("inner_walkforward_deflated_sharpe")
    if validation_metrics["avg_pips"] <= 0.0 or validation_metrics["median_pips"] < 0.0:
        reasons.append("holdout_edge")
    if validation_metrics["lower_confidence_pips"] <= 0.0:
        reasons.append("holdout_confidence")
    if validation_metrics["profit_factor"] < 1.10:
        reasons.append("holdout_profit_factor")
    if positive_blocks < max(1, int(math.ceil(0.60 * len(block_averages)))):
        reasons.append("time_block_stability")
    return {
        "stop_pips": stop_pips,
        "target_r": target_r,
        "target_pips": round(stop_pips * target_r, 3),
        "eligible": not reasons,
        "blocked_by": reasons,
        "sample_count": len(records),
        "split": (
            "purged three-fold expanding inner walk-forward on oldest 70% / "
            "newest 30% untouched outer holdout"
        ),
        "training": train_metrics,
        "development": _metrics(
            simulate_exit(row, stop_pips, target_r) for row in development
        ),
        "holdout": validation_metrics,
        "overall": overall_metrics,
        "positive_time_blocks": positive_blocks,
        "time_block_count": len(block_averages),
        "inner_fold_count": len(inner_folds),
        "purge_horizon_sec": horizon,
        "policy_trial_count": policy_trial_count,
    }


class StrategyExitFit:
    """Incremental SQLite outcome store with conservative exit recommendations."""

    def __init__(
        self,
        database_path: Path,
        state_path: Path,
        *,
        horizon_sec: int = 300,
        horizons_sec: Iterable[int] | None = None,
        refresh_sec: float = 300.0,
        refresh_new_rows: int = 100,
        max_fit_rows: int = 50000,
        fit_enabled: bool = True,
        record_enabled: bool = True,
    ) -> None:
        self.database_path = Path(database_path)
        self.state_path = Path(state_path)
        self.horizon_sec = int(horizon_sec)
        configured_horizons = {
            int(value)
            for value in (horizons_sec or (self.horizon_sec,))
            if int(value) > 0
        }
        configured_horizons.add(self.horizon_sec)
        self.horizons_sec = tuple(sorted(configured_horizons))
        self.refresh_sec = max(10.0, float(refresh_sec))
        self.refresh_new_rows = max(1, int(refresh_new_rows))
        self.max_fit_rows = max(1000, int(max_fit_rows))
        self.fit_enabled = bool(fit_enabled)
        self.record_enabled = bool(record_enabled)
        self.connection: sqlite3.Connection | None = None
        if self.record_enabled:
            self.database_path.parent.mkdir(parents=True, exist_ok=True)
            self.connection = sqlite3.connect(self.database_path, timeout=30.0)
            self.connection.execute("PRAGMA journal_mode=WAL")
            self.connection.execute("PRAGMA synchronous=NORMAL")
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS outcomes (
                    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL,
                    horizon_sec INTEGER NOT NULL,
                    observed_utc TEXT NOT NULL,
                    lane_id TEXT NOT NULL,
                    family TEXT NOT NULL,
                    profile TEXT NOT NULL,
                    model_id TEXT NOT NULL DEFAULT '',
                    input_timeframe TEXT NOT NULL DEFAULT '',
                    kind TEXT NOT NULL,
                    instrument TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    entry_time TEXT,
                    exit_time TEXT,
                    endpoint_pips REAL NOT NULL,
                    entry_spread_pips REAL NOT NULL DEFAULT 0,
                    max_favorable_pips REAL NOT NULL,
                    max_adverse_pips REAL NOT NULL,
                    first_positive_sec REAL,
                    path_samples INTEGER NOT NULL,
                    positive_path_samples INTEGER NOT NULL DEFAULT 0,
                    favorable_hits_json TEXT NOT NULL,
                    adverse_hits_json TEXT NOT NULL,
                    volatility_regime TEXT NOT NULL DEFAULT '',
                    UNIQUE(event_id, horizon_sec)
                )
                """
            )
            existing_columns = {
                str(row[1])
                for row in self.connection.execute("PRAGMA table_info(outcomes)")
            }
            migrations = {
                "volatility_regime": (
                    "ALTER TABLE outcomes ADD COLUMN volatility_regime TEXT NOT NULL DEFAULT ''"
                ),
                "model_id": (
                    "ALTER TABLE outcomes ADD COLUMN model_id TEXT NOT NULL DEFAULT ''"
                ),
                "input_timeframe": (
                    "ALTER TABLE outcomes ADD COLUMN input_timeframe TEXT NOT NULL DEFAULT ''"
                ),
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
            self.connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_exit_scope ON outcomes(horizon_sec, kind, family, lane_id)"
            )
            self.connection.commit()
        self.new_rows = 0
        self.last_refresh_monotonic = time.monotonic()
        self.state = self._load_state()
        self._state_mtime_ns = self._state_file_mtime_ns()
        self._last_state_check_monotonic = 0.0

    def _load_state(self) -> dict[str, Any]:
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
        except (OSError, json.JSONDecodeError):
            return {}

    def _state_file_mtime_ns(self) -> int:
        try:
            return self.state_path.stat().st_mtime_ns
        except OSError:
            return 0

    def _reload_state_if_changed(self) -> None:
        now = time.monotonic()
        if now - self._last_state_check_monotonic < 5.0:
            return
        self._last_state_check_monotonic = now
        current_mtime_ns = self._state_file_mtime_ns()
        if current_mtime_ns and current_mtime_ns != self._state_mtime_ns:
            self.state = self._load_state()
            self._state_mtime_ns = current_mtime_ns

    def observe(self, **row: Any) -> None:
        if self.connection is None or str(row.get("kind") or "") != "signal":
            return
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO outcomes (
                event_id, horizon_sec, observed_utc, lane_id, family, profile,
                model_id, input_timeframe, kind,
                instrument, direction, entry_time, exit_time, endpoint_pips,
                entry_spread_pips, max_favorable_pips, max_adverse_pips,
                first_positive_sec, path_samples, positive_path_samples,
                favorable_hits_json, adverse_hits_json, volatility_regime
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(row.get("event_id") or ""),
                int(_finite(row.get("horizon_sec"))),
                utc_now(),
                str(row.get("lane_id") or ""),
                str(row.get("family") or ""),
                str(row.get("profile") or ""),
                str(row.get("model_id") or row.get("lane_id") or ""),
                str(row.get("input_timeframe") or ""),
                str(row.get("kind") or ""),
                str(row.get("instrument") or ""),
                str(row.get("direction") or ""),
                str(row.get("entry_time") or ""),
                str(row.get("exit_time") or ""),
                _finite(row.get("endpoint_pips")),
                max(0.0, _finite(row.get("entry_spread_pips"))),
                _finite(row.get("max_favorable_pips")),
                _finite(row.get("max_adverse_pips")),
                None
                if row.get("first_positive_sec") is None
                else max(0.0, _finite(row.get("first_positive_sec"))),
                int(_finite(row.get("path_samples"))),
                int(_finite(row.get("positive_path_samples"))),
                json.dumps(row.get("favorable_hits") or {}, sort_keys=True),
                json.dumps(row.get("adverse_hits") or {}, sort_keys=True),
                str(row.get("volatility_regime") or ""),
            ),
        )
        self.new_rows += max(0, cursor.rowcount)

    def _records(self) -> list[dict[str, Any]]:
        if self.connection is None:
            raise RuntimeError("exit-fit records are unavailable in state-only mode")
        placeholders = ",".join("?" for _ in self.horizons_sec)
        max_row = int(
            self.connection.execute("SELECT COALESCE(MAX(row_id), 0) FROM outcomes").fetchone()[0]
        )
        # Keep the live fit bounded to a recent row-id range. The previous
        # horizon index could scan and sort the multi-gigabyte ledger.
        minimum_row = max(0, max_row - self.max_fit_rows * 4)
        cursor = self.connection.execute(
            f"""
            SELECT row_id, horizon_sec, lane_id, family, profile,
                   model_id, input_timeframe, kind, instrument, direction,
                   entry_time, endpoint_pips, entry_spread_pips,
                   max_favorable_pips, max_adverse_pips, first_positive_sec,
                   path_samples, positive_path_samples, favorable_hits_json,
                   adverse_hits_json, volatility_regime
            FROM outcomes NOT INDEXED
            WHERE row_id >= ?
              AND horizon_sec IN ({placeholders})
              AND kind = 'signal'
            ORDER BY row_id DESC
            LIMIT ?
            """,
            (minimum_row, *self.horizons_sec, self.max_fit_rows),
        )
        records: list[dict[str, Any]] = []
        for values in cursor.fetchall():
            try:
                favorable = json.loads(values[18])
                adverse = json.loads(values[19])
            except json.JSONDecodeError:
                favorable, adverse = {}, {}
            records.append(
                {
                    "row_id": values[0],
                    "horizon_sec": values[1],
                    "lane_id": values[2],
                    "family": values[3],
                    "profile": values[4],
                    "model_id": values[5],
                    "input_timeframe": values[6],
                    "kind": values[7],
                    "instrument": values[8],
                    "direction": values[9],
                    "entry_time": values[10],
                    "endpoint_pips": values[11],
                    "entry_spread_pips": values[12],
                    "max_favorable_pips": values[13],
                    "max_adverse_pips": values[14],
                    "first_positive_sec": values[15],
                    "path_samples": values[16],
                    "positive_path_samples": values[17],
                    "favorable_hits": favorable,
                    "adverse_hits": adverse,
                    "volatility_regime": values[20],
                    "session": _session_bucket(str(values[10] or "")),
                }
            )
        return records

    @staticmethod
    def _scope_key(*values: str) -> str:
        return "::".join(str(value) for value in values)

    @staticmethod
    def _fit_scopes(
        groups: dict[str, list[dict[str, Any]]],
        minimum_total: int,
        *,
        display_floor: int = 20,
        horizon_sec: int | None = None,
    ) -> dict[str, dict[str, Any]]:
        return {
            key: _fit_group(rows, minimum_total, horizon_sec=horizon_sec)
            for key, rows in groups.items()
            if len(rows) >= min(minimum_total, display_floor)
        }

    @staticmethod
    def _fit_quality_scopes(
        groups: dict[str, list[dict[str, Any]]],
        minimum_total: int,
        *,
        display_floor: int = 20,
        trial_count: int | None = None,
    ) -> dict[str, dict[str, Any]]:
        selected = {
            key: rows
            for key, rows in groups.items()
            if len(rows) >= min(minimum_total, display_floor)
        }
        effective_trials = max(1, int(trial_count or len(selected)))
        return {
            key: _fit_quality_group(
                rows,
                minimum_total,
                trial_count=effective_trials,
            )
            for key, rows in selected.items()
        }

    @staticmethod
    def _top_scopes(
        fits: dict[str, dict[str, Any]],
        label: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        return sorted(
            ({label: key, **value} for key, value in fits.items()),
            key=lambda row: (
                bool(row.get("eligible")),
                _finite(
                    (row.get("holdout") or {}).get("lower_confidence_pips"),
                    -999.0,
                ),
                int(row.get("sample_count") or 0),
            ),
            reverse=True,
        )[:limit]

    @staticmethod
    def _compact_quality_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
        keys = (
            "n",
            "avg_pips",
            "median_pips",
            "win_rate",
            "lower_confidence_pips",
            "positive_probability",
            "positive_probability_lower",
            "calibrated_positive_probability",
            "ever_positive_rate",
            "median_mfe_pips",
            "mfe_p25_pips",
            "median_mae_pips",
            "mae_p75_pips",
            "median_entry_spread_pips",
            "median_mfe_to_spread",
            "time_to_positive_n",
            "median_time_to_positive_sec",
            "p75_time_to_positive_sec",
            "positive_path_fraction",
        )
        return {key: metrics.get(key) for key in keys if key in metrics}

    @classmethod
    def _compact_quality_fit(cls, fit: dict[str, Any]) -> dict[str, Any]:
        keys = (
            "eligible",
            "negative_evidence",
            "blocked_by",
            "sample_count",
            "minimum_total_samples",
            "evidence_strength",
            "split",
            "calibrated_expected_net_pips",
            "calibrated_lower_net_pips",
            "calibrated_positive_probability",
            "positive_time_blocks",
            "time_block_count",
            "independent_holdout_blocks",
            "minimum_independent_blocks",
            "selection_adjusted_lower_net_pips",
            "deflated_sharpe_probability",
            "trial_count",
        )
        compact = {key: fit.get(key) for key in keys if key in fit}
        compact["holdout"] = cls._compact_quality_metrics(fit.get("holdout") or {})
        compact["overall"] = cls._compact_quality_metrics(fit.get("overall") or {})
        return compact

    def _fit_horizon(
        self,
        horizon_sec: int,
        records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        group_names = (
            "families",
            "lanes",
            "pairs",
            "regimes",
            "family_regimes",
            "lane_regimes",
            "pair_families",
            "pair_lanes",
            "pair_regimes",
            "pair_family_regimes",
            "pair_lane_regimes",
        )
        groups: dict[str, dict[str, list[dict[str, Any]]]] = {
            name: defaultdict(list)
            for name in group_names
        }
        quality_minimums = {
            "families": 120,
            "lanes": 60,
            "models": 60,
            "timeframes": 120,
            "pairs": 80,
            "regimes": 80,
            "sessions": 80,
            "family_regimes": 60,
            "lane_regimes": 40,
            "pair_families": 60,
            "pair_lanes": 40,
            "pair_models": 40,
            "pair_family_timeframes": 30,
            "model_timeframes": 50,
            "pair_model_timeframes": 30,
            "pair_timeframes": 50,
            "pair_regimes": 50,
            "pair_sessions": 50,
            "lane_sessions": 40,
            "model_sessions": 40,
            "pair_lane_regimes": 30,
            "pair_model_regimes": 30,
            "pair_lane_sessions": 30,
            "pair_model_sessions": 30,
            "pair_lane_directions": 30,
            "pair_model_directions": 30,
        }
        quality_groups: dict[str, dict[str, list[dict[str, Any]]]] = {
            name: defaultdict(list) for name in quality_minimums
        }
        for row in records:
            family = str(row.get("family") or "")
            lane_id = str(row.get("lane_id") or "")
            model_id = str(row.get("model_id") or lane_id)
            timeframe = str(row.get("input_timeframe") or "unknown")
            instrument = str(row.get("instrument") or "")
            regime = str(row.get("volatility_regime") or "unknown")
            session = str(row.get("session") or "unknown")
            direction = str(row.get("direction") or "unknown")
            groups["families"][family].append(row)
            groups["lanes"][lane_id].append(row)
            groups["pairs"][instrument].append(row)
            groups["regimes"][regime].append(row)
            groups["family_regimes"][self._scope_key(family, regime)].append(row)
            groups["lane_regimes"][self._scope_key(lane_id, regime)].append(row)
            groups["pair_families"][self._scope_key(instrument, family)].append(row)
            groups["pair_lanes"][self._scope_key(instrument, lane_id)].append(row)
            groups["pair_regimes"][self._scope_key(instrument, regime)].append(row)
            groups["pair_family_regimes"][
                self._scope_key(instrument, family, regime)
            ].append(row)
            groups["pair_lane_regimes"][
                self._scope_key(instrument, lane_id, regime)
            ].append(row)
            quality_groups["families"][family].append(row)
            quality_groups["lanes"][lane_id].append(row)
            quality_groups["models"][model_id].append(row)
            quality_groups["timeframes"][timeframe].append(row)
            quality_groups["pairs"][instrument].append(row)
            quality_groups["regimes"][regime].append(row)
            quality_groups["sessions"][session].append(row)
            quality_groups["family_regimes"][
                self._scope_key(family, regime)
            ].append(row)
            quality_groups["lane_regimes"][
                self._scope_key(lane_id, regime)
            ].append(row)
            quality_groups["pair_families"][
                self._scope_key(instrument, family)
            ].append(row)
            quality_groups["pair_lanes"][
                self._scope_key(instrument, lane_id)
            ].append(row)
            quality_groups["pair_models"][
                self._scope_key(instrument, model_id)
            ].append(row)
            quality_groups["pair_family_timeframes"][
                self._scope_key(instrument, family, timeframe)
            ].append(row)
            quality_groups["model_timeframes"][
                self._scope_key(model_id, timeframe)
            ].append(row)
            quality_groups["pair_model_timeframes"][
                self._scope_key(instrument, model_id, timeframe)
            ].append(row)
            quality_groups["pair_timeframes"][
                self._scope_key(instrument, timeframe)
            ].append(row)
            quality_groups["pair_regimes"][
                self._scope_key(instrument, regime)
            ].append(row)
            quality_groups["pair_sessions"][
                self._scope_key(instrument, session)
            ].append(row)
            quality_groups["lane_sessions"][
                self._scope_key(lane_id, session)
            ].append(row)
            quality_groups["model_sessions"][
                self._scope_key(model_id, session)
            ].append(row)
            quality_groups["pair_lane_regimes"][
                self._scope_key(instrument, lane_id, regime)
            ].append(row)
            quality_groups["pair_model_regimes"][
                self._scope_key(instrument, model_id, regime)
            ].append(row)
            quality_groups["pair_lane_sessions"][
                self._scope_key(instrument, lane_id, session)
            ].append(row)
            quality_groups["pair_model_sessions"][
                self._scope_key(instrument, model_id, session)
            ].append(row)
            quality_groups["pair_lane_directions"][
                self._scope_key(instrument, lane_id, direction)
            ].append(row)
            quality_groups["pair_model_directions"][
                self._scope_key(instrument, model_id, direction)
            ].append(row)

        minimums = {
            "families": 120,
            "lanes": 60,
            "pairs": 80,
            "regimes": 80,
            "family_regimes": 60,
            "lane_regimes": 40,
            "pair_families": 60,
            "pair_lanes": 40,
            "pair_regimes": 50,
            "pair_family_regimes": 40,
            "pair_lane_regimes": 30,
        }
        fits = {
            name: self._fit_scopes(
                scope_groups,
                minimums[name],
                display_floor=10 if name.endswith("_regimes") else 20,
                horizon_sec=horizon_sec,
            )
            for name, scope_groups in groups.items()
        }
        quality_trial_count = max(
            1,
            len(self.horizons_sec)
            * (
                1
                + sum(
                    sum(
                        len(rows) >= quality_minimums[name]
                        for rows in scope_groups.values()
                    )
                    for name, scope_groups in quality_groups.items()
                )
            ),
        )
        quality_fits = {
            name: self._fit_quality_scopes(
                scope_groups,
                quality_minimums[name],
                display_floor=(
                    1
                    if name == "pair_family_timeframes"
                    else quality_minimums[name]
                ),
                trial_count=quality_trial_count,
            )
            for name, scope_groups in quality_groups.items()
        }
        global_fit = (
            _fit_group(records, 240, horizon_sec=horizon_sec) if records else {}
        )
        global_quality = (
            _fit_quality_group(
                records,
                240,
                trial_count=quality_trial_count,
            )
            if records
            else {}
        )
        recommendations = {
            "global": global_fit if global_fit.get("eligible") else {},
            **{
                name: {
                    key: value
                    for key, value in scope_fits.items()
                    if value.get("eligible")
                }
                for name, scope_fits in fits.items()
            },
        }
        eligible_count = int(bool(recommendations["global"])) + sum(
            len(recommendations[name])
            for name in group_names
        )
        quality_evidence = {
            "global": self._compact_quality_fit(global_quality),
            **{
                name: {
                    key: self._compact_quality_fit(value)
                    for key, value in scope_fits.items()
                }
                for name, scope_fits in quality_fits.items()
            },
        }
        quality_eligible_count = int(bool(global_quality.get("eligible"))) + sum(
            sum(bool(value.get("eligible")) for value in scope_fits.values())
            for scope_fits in quality_fits.values()
        )
        return {
            "horizon_sec": int(horizon_sec),
            "sample_count": len(records),
            "global": global_fit,
            "top_families": self._top_scopes(fits["families"], "family", 12),
            "top_lanes": self._top_scopes(fits["lanes"], "lane_id", 20),
            "top_pairs": self._top_scopes(fits["pairs"], "instrument", 12),
            "top_regimes": self._top_scopes(fits["regimes"], "regime", 8),
            "eligible_scope_count": eligible_count,
            "prediction_quality_eligible_scope_count": quality_eligible_count,
            "prediction_quality_trial_count": quality_trial_count,
            "eligible": {
                "global": int(bool(recommendations["global"])),
                **{name: len(recommendations[name]) for name in group_names},
            },
            "recommendations": recommendations,
            "prediction_quality": global_quality,
            "prediction_quality_evidence": quality_evidence,
            "top_prediction_models": self._top_scopes(
                quality_fits["models"], "model_id", 20
            ),
            "top_prediction_pairs": self._top_scopes(
                quality_fits["pairs"], "instrument", 12
            ),
            "top_prediction_cells": self._top_scopes(
                quality_fits["pair_model_timeframes"],
                "pair_model_timeframe",
                40,
            ),
        }

    def fit(self) -> dict[str, Any]:
        self.connection.commit()
        records = self._records()
        records_by_horizon: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for row in records:
            records_by_horizon[int(row["horizon_sec"])].append(row)
        horizon_states = {
            str(horizon): self._fit_horizon(
                horizon,
                records_by_horizon.get(horizon, []),
            )
            for horizon in self.horizons_sec
        }
        legacy = horizon_states.get(str(self.horizon_sec)) or next(
            iter(horizon_states.values()),
            {},
        )
        fit_signal_by_horizon = {
            str(horizon): len(rows)
            for horizon, rows in sorted(records_by_horizon.items())
        }
        ledger_row_id_high_water = int(
            self.connection.execute("SELECT COALESCE(MAX(row_id), 0) FROM outcomes").fetchone()[0]
        )
        recommendations_by_horizon = {
            horizon: state.get("recommendations") or {}
            for horizon, state in horizon_states.items()
        }
        prediction_quality_by_horizon = {
            horizon: state.get("prediction_quality_evidence") or {}
            for horizon, state in horizon_states.items()
        }
        for horizon_state in horizon_states.values():
            horizon_state.pop("prediction_quality_evidence", None)
        eligible_horizons = sum(
            int((state.get("eligible_scope_count") or 0) > 0)
            for state in horizon_states.values()
        )
        quality_eligible_horizons = sum(
            int((state.get("prediction_quality_eligible_scope_count") or 0) > 0)
            for state in horizon_states.values()
        )
        state = {
            "schema_version": 4,
            "generated_at": utc_now(),
            "status": "ready" if eligible_horizons else "collecting",
            "database": str(self.database_path.resolve()),
            "horizon_sec": self.horizon_sec,
            "horizons_sec": list(self.horizons_sec),
            "method": (
                "stop/target selected by purged expanding inner walk-forward; "
                "eligibility requires positive newest 30% outer holdout, "
                "multiple-testing-adjusted net confidence, and stable time blocks; "
                "recommendations are selected by horizon, pair, lane/family, "
                "and volatility regime with conservative fallback. Prediction "
                "quality uses executable bid/ask net pips plus MFE, MAE, "
                "time-to-positive, path occupancy, pair, model, timeframe, "
                "UTC session, regime, and direction; calibration is always "
                "chronological, uses maturity-sized independent blocks and a "
                "DSR-style trial correction, and never promotes from in-sample "
                "metrics alone"
            ),
            "grid": {"stop_pips": list(STOP_GRID), "target_r": list(TARGET_R_GRID)},
            "counts": {
                "fit_signal_rows": len(records),
                "fit_signal_by_horizon": fit_signal_by_horizon,
                "ledger_row_id_high_water": ledger_row_id_high_water,
                "ledger_full_counts_status": "not_scanned_in_bounded_live_fit",
            },
            "eligible": {
                "horizons": eligible_horizons,
                "prediction_quality_horizons": quality_eligible_horizons,
                **(legacy.get("eligible") or {}),
            },
            "global": legacy.get("global") or {},
            "top_families": legacy.get("top_families") or [],
            "top_lanes": legacy.get("top_lanes") or [],
            "top_pairs": legacy.get("top_pairs") or [],
            "top_regimes": legacy.get("top_regimes") or [],
            "horizon_states": horizon_states,
            "recommendations": legacy.get("recommendations") or {},
            "recommendations_by_horizon": recommendations_by_horizon,
            "prediction_quality": legacy.get("prediction_quality") or {},
            "top_prediction_models": legacy.get("top_prediction_models") or [],
            "top_prediction_pairs": legacy.get("top_prediction_pairs") or [],
            "top_prediction_cells": legacy.get("top_prediction_cells") or [],
            "prediction_quality_by_horizon": prediction_quality_by_horizon,
        }
        _atomic_json(self.state_path, state)
        write_pair_family_matrix(
            state,
            timeframe_state_path=self.state_path.with_name(
                "timeframe_matrix_calibration_v1.json"
            ),
            output_path=self.state_path.with_name(
                "pair_family_timeframe_horizon_v1.json"
            ),
        )
        self.state = state
        self._state_mtime_ns = self._state_file_mtime_ns()
        self.new_rows = 0
        self.last_refresh_monotonic = time.monotonic()
        return state

    def flush(self, *, force_fit: bool = False) -> None:
        if self.connection is None:
            return
        self.connection.commit()
        due = time.monotonic() - self.last_refresh_monotonic >= self.refresh_sec
        if self.fit_enabled and (force_fit or (self.new_rows >= self.refresh_new_rows and due)):
            self.fit()

    def recommendation(
        self,
        lane_id: str,
        family: str,
        instrument: str = "",
        horizon_sec: int | None = None,
        volatility_regime: str = "",
    ) -> dict[str, Any] | None:
        self._reload_state_if_changed()
        horizon = int(horizon_sec or self.horizon_sec)
        recommendations = (
            (self.state.get("recommendations_by_horizon") or {}).get(str(horizon))
            or (
                self.state.get("recommendations")
                if horizon == self.horizon_sec
                else {}
            )
            or {}
        )
        regime = volatility_regime or "unknown"
        scopes = (
            (
                "pair_lane_regime",
                "pair_lane_regimes",
                self._scope_key(instrument, lane_id, regime),
            ),
            (
                "pair_family_regime",
                "pair_family_regimes",
                self._scope_key(instrument, family, regime),
            ),
            (
                "pair_regime",
                "pair_regimes",
                self._scope_key(instrument, regime),
            ),
            ("pair_lane", "pair_lanes", self._scope_key(instrument, lane_id)),
            ("pair_family", "pair_families", self._scope_key(instrument, family)),
            ("pair", "pairs", instrument),
            ("lane_regime", "lane_regimes", self._scope_key(lane_id, regime)),
            (
                "family_regime",
                "family_regimes",
                self._scope_key(family, regime),
            ),
            ("lane", "lanes", lane_id),
            ("family", "families", family),
            ("regime", "regimes", regime),
        )
        for scope, bucket, key in scopes:
            selected = (recommendations.get(bucket) or {}).get(key)
            if isinstance(selected, dict) and selected.get("eligible"):
                return {
                    **selected,
                    "scope": scope,
                    "scope_key": key,
                    "horizon_sec": horizon,
                }
        selected = recommendations.get("global")
        if isinstance(selected, dict) and selected.get("eligible"):
            return {
                **selected,
                "scope": "global",
                "scope_key": "global",
                "horizon_sec": horizon,
            }
        return None

    def quality_evidence(
        self,
        lane_id: str,
        family: str,
        instrument: str = "",
        horizon_sec: int | None = None,
        volatility_regime: str = "",
        *,
        model_id: str = "",
        input_timeframe: str = "",
        direction: str = "",
        entry_time: str = "",
        min_samples: int = 30,
    ) -> dict[str, Any] | None:
        """Return the most specific chronological executable-return calibration."""

        self._reload_state_if_changed()
        horizon = int(horizon_sec or self.horizon_sec)
        evidence = (
            (self.state.get("prediction_quality_by_horizon") or {}).get(str(horizon))
            or {}
        )
        if not evidence:
            return None
        model = model_id or lane_id
        timeframe = input_timeframe or "unknown"
        regime = volatility_regime or "unknown"
        side = direction or "unknown"
        session = _session_bucket(entry_time)
        scopes = (
            (
                "pair_model_timeframe",
                "pair_model_timeframes",
                self._scope_key(instrument, model, timeframe),
            ),
            ("pair_model_session", "pair_model_sessions", self._scope_key(instrument, model, session)),
            ("pair_lane_session", "pair_lane_sessions", self._scope_key(instrument, lane_id, session)),
            ("pair_model_regime", "pair_model_regimes", self._scope_key(instrument, model, regime)),
            ("pair_lane_regime", "pair_lane_regimes", self._scope_key(instrument, lane_id, regime)),
            ("pair_model_direction", "pair_model_directions", self._scope_key(instrument, model, side)),
            ("pair_lane_direction", "pair_lane_directions", self._scope_key(instrument, lane_id, side)),
            ("pair_model", "pair_models", self._scope_key(instrument, model)),
            ("pair_lane", "pair_lanes", self._scope_key(instrument, lane_id)),
            ("pair_family", "pair_families", self._scope_key(instrument, family)),
            ("pair_timeframe", "pair_timeframes", self._scope_key(instrument, timeframe)),
            ("pair_session", "pair_sessions", self._scope_key(instrument, session)),
            ("pair_regime", "pair_regimes", self._scope_key(instrument, regime)),
            ("model_session", "model_sessions", self._scope_key(model, session)),
            (
                "model_timeframe",
                "model_timeframes",
                self._scope_key(model, timeframe),
            ),
            ("lane_session", "lane_sessions", self._scope_key(lane_id, session)),
            ("family_regime", "family_regimes", self._scope_key(family, regime)),
            ("lane_regime", "lane_regimes", self._scope_key(lane_id, regime)),
            ("model", "models", model),
            ("lane", "lanes", lane_id),
            ("family", "families", family),
            ("timeframe", "timeframes", timeframe),
            ("pair", "pairs", instrument),
            ("session", "sessions", session),
            ("regime", "regimes", regime),
        )
        minimum = max(1, int(min_samples))
        for scope, bucket, key in scopes:
            selected = (evidence.get(bucket) or {}).get(key)
            if isinstance(selected, dict) and int(selected.get("sample_count") or 0) >= minimum:
                return {
                    **selected,
                    "scope": scope,
                    "scope_key": key,
                    "horizon_sec": horizon,
                    "session": session,
                }
        selected = evidence.get("global")
        if isinstance(selected, dict) and int(selected.get("sample_count") or 0) >= minimum:
            return {
                **selected,
                "scope": "global",
                "scope_key": "global",
                "horizon_sec": horizon,
                "session": session,
            }
        return None

    def close(self) -> None:
        if self.connection is None:
            return
        try:
            self.flush(force_fit=bool(self.new_rows and self.fit_enabled))
        finally:
            self.connection.close()


__all__ = [
    "PATH_LEVELS",
    "STOP_GRID",
    "TARGET_R_GRID",
    "StrategyExitFit",
    "level_key",
    "simulate_exit",
]
