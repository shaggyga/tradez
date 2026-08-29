#!/usr/bin/env python3
"""Wide multi-timeframe SMA features and a validated candidate-signal filter."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np

try:
    import joblib
except ImportError:  # pragma: no cover - live collection works without sklearn/joblib.
    joblib = None


SCHEMA_VERSION = 1
TIMEFRAME_MINUTES = {
    "M1": 1,
    "M5": 5,
    "M10": 10,
    "M15": 15,
    "M30": 30,
    "H1": 60,
    "H2": 120,
    "H3": 180,
    "H4": 240,
}
TIMEFRAME_SERIES_KEYS = {
    "M1": "closes",
    "M5": "m5_closes",
    "M10": "m10_closes",
    "M15": "m15_closes",
    "M30": "m30_closes",
    "H1": "h1_closes",
    "H2": "h2_closes",
    "H3": "h3_closes",
    "H4": "h4_closes",
}

# The period ceiling follows the history already fetched by the live lab. This
# keeps the filter broad without adding latency to the minute-close refresh.
TIMEFRAME_PERIODS = {
    "M1": (2, 3, 4, 5, 7, 8, 9, 10, 12, 13, 15, 20, 21, 25, 30, 40, 50, 75, 100),
    "M5": (2, 3, 4, 5, 7, 8, 9, 10, 12, 13, 15, 20, 21, 25, 30, 40, 50, 75, 100, 150, 200),
    "M10": (2, 3, 4, 5, 7, 8, 9, 10, 12, 13, 15, 20, 21, 25, 30, 40, 50, 75, 100),
    "M15": (2, 3, 4, 5, 7, 8, 9, 10, 12, 13, 15, 20, 21, 25, 30, 40, 50, 75, 100),
    "M30": (2, 3, 4, 5, 7, 8, 9, 10, 12, 13, 15, 20, 21, 25, 30, 40, 50),
    "H1": (2, 3, 4, 5, 7, 8, 9, 10, 12, 13, 15, 20, 21, 25, 30, 40, 50, 75, 100),
    "H2": (2, 3, 4, 5, 7, 8, 9, 10, 12, 13, 15, 20, 21, 25, 30, 40, 50),
    "H3": (2, 3, 4, 5, 7, 8, 9, 10, 12, 13, 15, 20, 21, 25, 30, 40, 50),
    "H4": (2, 3, 4, 5, 7, 8, 9, 10, 12, 13, 15, 20, 21, 25, 30),
}
CANONICAL_SMA_PAIRS = (
    (3, 5),
    (5, 8),
    (8, 13),
    (8, 21),
    (10, 20),
    (13, 21),
    (20, 50),
    (21, 50),
    (30, 75),
    (50, 100),
    (50, 200),
    (100, 200),
)
CROSS_TIMEFRAME_PERIODS = (8, 20, 50)
CROSS_TIMEFRAME_PAIRS = ((5, 8), (8, 21), (20, 50))


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    return numeric if math.isfinite(numeric) else default


def _direction_sign(direction: str) -> float:
    side = str(direction or "").strip().lower()
    if side == "buy":
        return 1.0
    if side == "sell":
        return -1.0
    raise ValueError("direction must be buy or sell")


def _rolling_mean(values: np.ndarray, period: int) -> np.ndarray:
    output = np.full(len(values), np.nan, dtype=np.float64)
    if period <= 0 or len(values) < period:
        return output
    cumulative = np.cumsum(np.insert(values.astype(np.float64, copy=False), 0, 0.0))
    output[period - 1 :] = (cumulative[period:] - cumulative[:-period]) / period
    return output


def _atr_proxy_pips(values: np.ndarray, pip: float) -> float:
    if len(values) < 2:
        return 0.1
    moves = np.abs(np.diff(values[-16:]))
    return max(0.1, float(np.mean(moves)) / max(pip, 1e-12))


def _cross_age(fast: np.ndarray, slow: np.ndarray, limit: int = 128) -> int:
    valid = np.isfinite(fast) & np.isfinite(slow)
    indexes = np.flatnonzero(valid)
    if not len(indexes):
        return limit
    indexes = indexes[-max(2, int(limit) + 1) :]
    side = fast[indexes] >= slow[indexes]
    changes = np.flatnonzero(side[1:] != side[:-1])
    return min(limit, len(side) - 1 - int(changes[-1])) if len(changes) else min(limit, len(side) - 1)


def _pair_list(periods: Iterable[int]) -> tuple[tuple[int, int], ...]:
    ordered = tuple(sorted({int(value) for value in periods}))
    available = set(ordered)
    pairs = {
        (fast, slow)
        for fast, slow in CANONICAL_SMA_PAIRS
        if fast in available and slow in available
    }
    pairs.update(zip(ordered, ordered[1:]))
    return tuple(
        sorted(
            pairs,
            key=lambda pair: (pair[1], pair[0]),
        )
    )


def sma_feature_names() -> tuple[str, ...]:
    names: list[str] = []
    for timeframe, periods in TIMEFRAME_PERIODS.items():
        for period in periods:
            prefix = f"sma__{timeframe}__p{period}"
            names.extend(
                (
                    f"{prefix}__distance_atr",
                    f"{prefix}__slope1_atr",
                    f"{prefix}__slope3_atr",
                )
            )
        for fast, slow in _pair_list(periods):
            prefix = f"sma__{timeframe}__p{fast}_{slow}"
            names.extend(
                (
                    f"{prefix}__gap_atr",
                    f"{prefix}__velocity1_atr",
                    f"{prefix}__cross_age_log",
                    f"{prefix}__recent_cross_support",
                )
            )
        names.extend(
            (
                f"sma__{timeframe}__price_alignment_fraction",
                f"sma__{timeframe}__slope_alignment_fraction",
                f"sma__{timeframe}__order_alignment_fraction",
                f"sma__{timeframe}__mean_distance_atr",
                f"sma__{timeframe}__distance_dispersion_atr",
            )
        )
    for period in CROSS_TIMEFRAME_PERIODS:
        names.extend(
            (
                f"sma__cross_tf__p{period}__alignment_fraction",
                f"sma__cross_tf__p{period}__mean_distance_atr",
                f"sma__cross_tf__p{period}__dispersion_atr",
            )
        )
    for fast, slow in CROSS_TIMEFRAME_PAIRS:
        names.extend(
            (
                f"sma__cross_tf__p{fast}_{slow}__alignment_fraction",
                f"sma__cross_tf__p{fast}_{slow}__mean_gap_atr",
                f"sma__cross_tf__p{fast}_{slow}__dispersion_atr",
            )
        )
    names.extend(
        (
            "sma__global__price_alignment_fraction",
            "sma__global__slope_alignment_fraction",
            "sma__global__order_alignment_fraction",
            "sma__global__timeframe_disagreement",
            "sma__context__spread_atr",
        )
    )
    return tuple(names)


SMA_FEATURE_NAMES = sma_feature_names()


def build_sma_signal_vector(
    features: dict[str, Any],
    direction: str,
    *,
    spread_pips: float | None = None,
) -> dict[str, float]:
    """Build direction-conditioned SMA state using only completed input series."""

    sign = _direction_sign(direction)
    pip = max(1e-12, finite(features.get("pip"), 0.0001))
    spread = max(
        0.0,
        finite(
            spread_pips,
            finite(
                features.get("live_spread_pips"),
                finite(features.get("current_candle_spread_pips")),
            ),
        ),
    )
    vector: dict[str, float] = {}
    global_price: list[float] = []
    global_slope: list[float] = []
    global_order: list[float] = []
    timeframe_price_support: list[float] = []

    for timeframe, series_key in TIMEFRAME_SERIES_KEYS.items():
        raw_values = features.get(series_key) or []
        values = np.asarray([finite(value, math.nan) for value in raw_values], dtype=np.float64)
        values = values[np.isfinite(values)]
        periods = TIMEFRAME_PERIODS[timeframe]
        if len(values) < min(periods) + 3:
            continue
        atr_pips = _atr_proxy_pips(values, pip)
        denominator = max(0.1, atr_pips) * pip
        means = {period: _rolling_mean(values, period) for period in periods}
        distances: list[float] = []
        slopes: list[float] = []
        ordering: list[float] = []

        for period, mean in means.items():
            if len(mean) < 4 or not np.isfinite(mean[-4:]).all():
                continue
            distance = sign * (values[-1] - mean[-1]) / denominator
            slope1 = sign * (mean[-1] - mean[-2]) / denominator
            slope3 = sign * (mean[-1] - mean[-4]) / denominator
            prefix = f"sma__{timeframe}__p{period}"
            vector[f"{prefix}__distance_atr"] = float(distance)
            vector[f"{prefix}__slope1_atr"] = float(slope1)
            vector[f"{prefix}__slope3_atr"] = float(slope3)
            distances.append(distance)
            slopes.append(slope3)

        for fast_period, slow_period in _pair_list(periods):
            fast = means[fast_period]
            slow = means[slow_period]
            if len(fast) < 2 or not np.isfinite((fast[-1], fast[-2], slow[-1], slow[-2])).all():
                continue
            gap = sign * (fast[-1] - slow[-1]) / denominator
            velocity = sign * ((fast[-1] - slow[-1]) - (fast[-2] - slow[-2])) / denominator
            age = _cross_age(fast, slow)
            aligned_side = sign * (1.0 if fast[-1] >= slow[-1] else -1.0)
            prefix = f"sma__{timeframe}__p{fast_period}_{slow_period}"
            vector[f"{prefix}__gap_atr"] = float(gap)
            vector[f"{prefix}__velocity1_atr"] = float(velocity)
            vector[f"{prefix}__cross_age_log"] = math.log1p(age)
            vector[f"{prefix}__recent_cross_support"] = float(aligned_side / (1.0 + age))
            ordering.append(gap)

        if distances:
            price_fraction = float(np.mean(np.asarray(distances) > 0.0))
            vector[f"sma__{timeframe}__price_alignment_fraction"] = price_fraction
            vector[f"sma__{timeframe}__mean_distance_atr"] = float(np.mean(distances))
            vector[f"sma__{timeframe}__distance_dispersion_atr"] = float(np.std(distances))
            global_price.extend(distances)
            timeframe_price_support.append(price_fraction)
        if slopes:
            vector[f"sma__{timeframe}__slope_alignment_fraction"] = float(
                np.mean(np.asarray(slopes) > 0.0)
            )
            global_slope.extend(slopes)
        if ordering:
            vector[f"sma__{timeframe}__order_alignment_fraction"] = float(
                np.mean(np.asarray(ordering) > 0.0)
            )
            global_order.extend(ordering)

    for period in CROSS_TIMEFRAME_PERIODS:
        values = [
            vector[name]
            for timeframe in TIMEFRAME_MINUTES
            if (name := f"sma__{timeframe}__p{period}__distance_atr") in vector
        ]
        if values:
            prefix = f"sma__cross_tf__p{period}"
            vector[f"{prefix}__alignment_fraction"] = float(np.mean(np.asarray(values) > 0.0))
            vector[f"{prefix}__mean_distance_atr"] = float(np.mean(values))
            vector[f"{prefix}__dispersion_atr"] = float(np.std(values))
    for fast, slow in CROSS_TIMEFRAME_PAIRS:
        values = [
            vector[name]
            for timeframe in TIMEFRAME_MINUTES
            if (name := f"sma__{timeframe}__p{fast}_{slow}__gap_atr") in vector
        ]
        if values:
            prefix = f"sma__cross_tf__p{fast}_{slow}"
            vector[f"{prefix}__alignment_fraction"] = float(np.mean(np.asarray(values) > 0.0))
            vector[f"{prefix}__mean_gap_atr"] = float(np.mean(values))
            vector[f"{prefix}__dispersion_atr"] = float(np.std(values))

    if global_price:
        vector["sma__global__price_alignment_fraction"] = float(
            np.mean(np.asarray(global_price) > 0.0)
        )
    if global_slope:
        vector["sma__global__slope_alignment_fraction"] = float(
            np.mean(np.asarray(global_slope) > 0.0)
        )
    if global_order:
        vector["sma__global__order_alignment_fraction"] = float(
            np.mean(np.asarray(global_order) > 0.0)
        )
    if timeframe_price_support:
        vector["sma__global__timeframe_disagreement"] = float(
            np.std(timeframe_price_support)
        )
    m1_values = np.asarray(
        [finite(value, math.nan) for value in (features.get("closes") or [])],
        dtype=np.float64,
    )
    m1_values = m1_values[np.isfinite(m1_values)]
    vector["sma__context__spread_atr"] = spread / _atr_proxy_pips(m1_values, pip)
    return {
        name: round(float(value), 8)
        for name, value in vector.items()
        if name in SMA_FEATURE_NAMES and math.isfinite(float(value))
    }


def snapshot_identity(instrument: str, origin_time: str, direction: str) -> str:
    raw = f"{instrument}|{origin_time}|{direction}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:32]


class SmaSignalFilterStore:
    """Normalized candidate ledger with one compressed SMA vector per market state."""

    def __init__(
        self,
        database_path: Path,
        *,
        batch_size: int = 2048,
        busy_timeout_ms: int = 3000,
    ) -> None:
        self.database_path = Path(database_path)
        self.batch_size = max(64, int(batch_size))
        self.busy_timeout_ms = max(100, int(busy_timeout_ms))
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(
            self.database_path,
            timeout=self.busy_timeout_ms / 1000.0,
        )
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS snapshots (
                snapshot_id TEXT PRIMARY KEY,
                instrument TEXT NOT NULL,
                origin_time TEXT NOT NULL,
                direction TEXT NOT NULL,
                feature_count INTEGER NOT NULL,
                features_zlib BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS candidates (
                event_id TEXT PRIMARY KEY,
                snapshot_id TEXT NOT NULL,
                lane_id TEXT NOT NULL,
                family TEXT NOT NULL,
                profile TEXT NOT NULL,
                kind TEXT NOT NULL,
                entry_time TEXT NOT NULL,
                entry_spread_pips REAL NOT NULL,
                FOREIGN KEY(snapshot_id) REFERENCES snapshots(snapshot_id)
            );
            CREATE INDEX IF NOT EXISTS idx_sma_candidate_scope
                ON candidates(kind, family, entry_time);
            CREATE TABLE IF NOT EXISTS outcomes (
                row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                horizon_sec INTEGER NOT NULL,
                outcome_time TEXT NOT NULL,
                net_pips REAL NOT NULL,
                max_favorable_pips REAL NOT NULL,
                max_adverse_pips REAL NOT NULL,
                UNIQUE(event_id, horizon_sec),
                FOREIGN KEY(event_id) REFERENCES candidates(event_id)
            );
            CREATE INDEX IF NOT EXISTS idx_sma_outcome_horizon
                ON outcomes(horizon_sec, row_id);
            """
        )
        self.connection.commit()
        self.pending_snapshots: dict[str, tuple[Any, ...]] = {}
        self.pending_candidates: list[tuple[Any, ...]] = []
        self.pending_outcomes: list[tuple[Any, ...]] = []
        self.committed_candidates = 0
        self.committed_outcomes = 0
        self.flush_busy_count = 0
        self.last_flush_error = ""
        self.next_flush_retry_epoch = 0.0

    def register_candidate(
        self,
        *,
        event_id: str,
        instrument: str,
        origin_time: str,
        direction: str,
        lane_id: str,
        family: str,
        profile: str,
        kind: str,
        entry_time: str,
        entry_spread_pips: float,
        features: dict[str, float],
    ) -> str:
        # The completed-bar state is shared across lanes in one evaluation
        # cycle, while entry_time keeps quote/spread context from being reused
        # by a later restart on the same candle.
        snapshot_id = snapshot_identity(
            instrument,
            f"{origin_time}|{entry_time}",
            direction,
        )
        if snapshot_id not in self.pending_snapshots:
            encoded = json.dumps(
                features,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            self.pending_snapshots[snapshot_id] = (
                snapshot_id,
                str(instrument),
                str(origin_time),
                str(direction),
                len(features),
                sqlite3.Binary(zlib.compress(encoded, level=6)),
            )
        self.pending_candidates.append(
            (
                str(event_id),
                snapshot_id,
                str(lane_id),
                str(family),
                str(profile),
                str(kind),
                str(entry_time),
                finite(entry_spread_pips),
            )
        )
        self.flush()
        return snapshot_id

    def observe(
        self,
        event_id: str,
        horizon_sec: float,
        outcome_time: str,
        net_pips: float,
        max_favorable_pips: float,
        max_adverse_pips: float,
    ) -> None:
        self.pending_outcomes.append(
            (
                str(event_id),
                int(horizon_sec),
                str(outcome_time),
                finite(net_pips),
                finite(max_favorable_pips),
                finite(max_adverse_pips),
            )
        )
        self.flush()

    def flush(self, *, force: bool = False) -> bool:
        pending_count = len(self.pending_candidates) + len(self.pending_outcomes)
        if not force and pending_count < self.batch_size:
            return True
        now = time.monotonic()
        if not force and now < self.next_flush_retry_epoch:
            return False

        snapshot_items = list(self.pending_snapshots.items())
        candidates = list(self.pending_candidates)
        outcomes = list(self.pending_outcomes)
        candidate_changes = 0
        outcome_changes = 0
        try:
            if snapshot_items:
                self.connection.executemany(
                    """
                    INSERT OR IGNORE INTO snapshots(
                        snapshot_id, instrument, origin_time, direction,
                        feature_count, features_zlib
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [value for _, value in snapshot_items],
                )
            if candidates:
                before = self.connection.total_changes
                self.connection.executemany(
                    """
                    INSERT OR IGNORE INTO candidates(
                        event_id, snapshot_id, lane_id, family, profile, kind,
                        entry_time, entry_spread_pips
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    candidates,
                )
                candidate_changes = max(
                    0, self.connection.total_changes - before
                )
            if outcomes:
                before = self.connection.total_changes
                self.connection.executemany(
                    """
                    INSERT OR IGNORE INTO outcomes(
                        event_id, horizon_sec, outcome_time, net_pips,
                        max_favorable_pips, max_adverse_pips
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    outcomes,
                )
                outcome_changes = max(
                    0, self.connection.total_changes - before
                )
            self.connection.commit()
        except sqlite3.OperationalError as exc:
            self.connection.rollback()
            if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                raise
            self.flush_busy_count += 1
            self.last_flush_error = str(exc)
            self.next_flush_retry_epoch = time.monotonic() + 1.0
            return False

        for key, _ in snapshot_items:
            self.pending_snapshots.pop(key, None)
        if candidates:
            del self.pending_candidates[: len(candidates)]
        if outcomes:
            del self.pending_outcomes[: len(outcomes)]
        self.committed_candidates += candidate_changes
        self.committed_outcomes += outcome_changes
        self.last_flush_error = ""
        self.next_flush_retry_epoch = 0.0
        return True

    def summary(self) -> dict[str, Any]:
        return {
            "database": str(self.database_path.resolve()),
            "schema_version": SCHEMA_VERSION,
            "feature_count": len(SMA_FEATURE_NAMES),
            "buffered_candidates": len(self.pending_candidates),
            "buffered_outcomes": len(self.pending_outcomes),
            "committed_candidates": self.committed_candidates,
            "committed_outcomes": self.committed_outcomes,
            "flush_busy_count": self.flush_busy_count,
            "last_flush_error": self.last_flush_error,
        }

    def close(self) -> None:
        try:
            self.flush(force=True)
        finally:
            self.connection.close()


class SmaSignalFilterModel:
    """Hot-reload a fitted SMA filter and score every configured horizon."""

    def __init__(self, artifact_path: Path) -> None:
        self.artifact_path = Path(artifact_path)
        self.modified_ns = -2
        self.artifact: dict[str, Any] = {}
        self.load_error = ""
        self._reload()

    def _reload(self) -> None:
        try:
            modified_ns = self.artifact_path.stat().st_mtime_ns
        except OSError:
            modified_ns = -1
        if modified_ns == self.modified_ns:
            return
        self.modified_ns = modified_ns
        if modified_ns < 0 or joblib is None:
            self.artifact = {}
            self.load_error = "artifact_missing" if modified_ns < 0 else "joblib_unavailable"
            return
        try:
            payload = joblib.load(self.artifact_path)
        except Exception as exc:  # pragma: no cover - corrupted external artifact.
            self.artifact = {}
            self.load_error = f"{type(exc).__name__}: {exc}"
            return
        if not isinstance(payload, dict):
            self.artifact = {}
            self.load_error = "invalid_artifact"
            return
        self.artifact = payload
        self.load_error = ""

    def predict_many(
        self,
        vectors: dict[Any, dict[str, float]],
    ) -> dict[Any, dict[str, Any]]:
        self._reload()
        models = self.artifact.get("models") or {}
        if not vectors:
            return {}
        if not models:
            return {
                key: {
                    "ready": False,
                    "reason": self.load_error or "no_fitted_horizons",
                    "feature_count": len(vector),
                }
                for key, vector in vectors.items()
            }
        keys = list(vectors)
        curves: dict[Any, dict[str, dict[str, Any]]] = {
            key: {} for key in keys
        }
        for horizon_text, spec in models.items():
            if not isinstance(spec, dict):
                continue
            estimator = spec.get("estimator")
            feature_names = tuple(spec.get("feature_names") or SMA_FEATURE_NAMES)
            if estimator is None:
                continue
            matrix = np.asarray(
                [
                    [
                        finite(vectors[key].get(name), math.nan)
                        for name in feature_names
                    ]
                    for key in keys
                ],
                dtype=np.float64,
            )
            try:
                if str(spec.get("score_kind") or "probability") == "probability":
                    scores = np.asarray(
                        estimator.predict_proba(matrix),
                        dtype=np.float64,
                    )[:, 1]
                else:
                    scores = np.asarray(
                        estimator.predict(matrix),
                        dtype=np.float64,
                    ).reshape(-1)
            except Exception:
                continue
            threshold = finite(spec.get("threshold"), 0.5)
            eligible = bool(spec.get("account_eligible"))
            for key, score_value in zip(keys, scores, strict=True):
                score = float(score_value)
                curves[key][str(int(finite(horizon_text)))] = {
                    "horizon_sec": int(finite(horizon_text)),
                    "score": round(score, 8),
                    "score_kind": str(
                        spec.get("score_kind") or "probability"
                    ),
                    "threshold": round(threshold, 8),
                    "accepted": bool(score >= threshold),
                    "account_eligible": eligible,
                    "status": (
                        "validated_filter"
                        if eligible
                        else "shadow_diagnostic"
                    ),
                    "model_name": str(spec.get("model_name") or ""),
                    "holdout_summary": {
                        name: (spec.get("holdout") or {}).get(name)
                        for name in (
                            "n",
                            "effective_n",
                            "average_net_pips",
                            "lower_confidence_net_pips",
                            "average_net_lift_pips",
                            "positive_pair_fraction",
                        )
                        if (spec.get("holdout") or {}).get(name) is not None
                    },
                }
        return {
            key: {
                "ready": bool(curves[key]),
                "generated_at": self.artifact.get("generated_at"),
                "feature_count": len(vectors[key]),
                "curve": curves[key],
                "account_eligible_horizons": [
                    int(horizon)
                    for horizon, row in curves[key].items()
                    if row.get("account_eligible")
                ],
            }
            for key in keys
        }

    def predict(self, vector: dict[str, float]) -> dict[str, Any]:
        return self.predict_many({0: vector})[0]


def filter_point_for_horizon(
    forecast: dict[str, Any] | None,
    horizon_sec: int,
) -> dict[str, Any]:
    if not isinstance(forecast, dict):
        return {}
    curve = forecast.get("curve") or {}
    if not isinstance(curve, dict) or not curve:
        return {}
    exact = curve.get(str(int(horizon_sec)))
    if isinstance(exact, dict):
        return exact
    requested = max(1, int(horizon_sec))
    candidates = [
        (abs(math.log(max(1, int(finite(key))) / requested)), value)
        for key, value in curve.items()
        if isinstance(value, dict) and int(finite(key)) > 0
    ]
    return min(candidates, key=lambda item: item[0])[1] if candidates else {}


def validated_filter_weight(
    forecast: dict[str, Any] | None,
    horizon_sec: int,
) -> float:
    """Return a neutral weight unless an untouched holdout enabled the filter."""

    point = filter_point_for_horizon(forecast, horizon_sec)
    if not point or not point.get("account_eligible"):
        return 1.0
    score = finite(point.get("score"))
    threshold = finite(point.get("threshold"), 0.5)
    if str(point.get("score_kind") or "probability") != "probability":
        scale = max(0.25, abs(threshold), abs(score))
        relative = math.tanh((score - threshold) / scale)
        return 0.75 + 0.75 * max(0.0, relative) if score >= threshold else max(
            0.05,
            0.35 * (1.0 + relative),
        )
    if score >= threshold:
        headroom = (score - threshold) / max(1e-9, 1.0 - threshold)
        return min(1.5, 0.75 + 0.75 * max(0.0, headroom))
    return max(0.05, 0.35 * score / max(threshold, 1e-9))


__all__ = [
    "CANONICAL_SMA_PAIRS",
    "CROSS_TIMEFRAME_PAIRS",
    "CROSS_TIMEFRAME_PERIODS",
    "SMA_FEATURE_NAMES",
    "SmaSignalFilterModel",
    "SmaSignalFilterStore",
    "TIMEFRAME_MINUTES",
    "TIMEFRAME_PERIODS",
    "TIMEFRAME_SERIES_KEYS",
    "build_sma_signal_vector",
    "filter_point_for_horizon",
    "sma_feature_names",
    "snapshot_identity",
    "validated_filter_weight",
]
