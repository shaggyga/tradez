#!/usr/bin/env python3
"""Capture OANDA price events and learn causal sub-minute pattern forecasts.

OANDA practice pricing is sampled by the broker at up to four updates per
second per instrument. This lab stores every received quote for replay and
learns live-only pattern statistics at fixed intrasecond and next-minute
boundary horizons. It also runs online equation forecasts from lightly smoothed
quote deltas. It never places orders and never treats M1 history as sub-second
observations.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import os
import sqlite3
import statistics
import tempfile
import time
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

try:
    from oanda_pattern_count_forecast import add_observation, empty_stats, parse_time_ns, safe_float, stats_rate
    from oanda_practice_eurusd_micro_scalper import DEFAULT_CREDS, DEFAULT_LOG_DIR, STREAM_URL
    from oanda_practice_pair_rotation_scalper import read_credentials, tradeable_currency_instruments
except ModuleNotFoundError:
    from trad.oanda_pattern_count_forecast import add_observation, empty_stats, parse_time_ns, safe_float, stats_rate
    from trad.oanda_practice_eurusd_micro_scalper import DEFAULT_CREDS, DEFAULT_LOG_DIR, STREAM_URL
    from trad.oanda_practice_pair_rotation_scalper import read_credentials, tradeable_currency_instruments


ROOT = Path(__file__).resolve().parent
STATE_DIR = ROOT / "data" / "oanda_training_manager" / "state"
DEFAULT_STATE = STATE_DIR / "micro_pattern_live_v1.json.gz"
DEFAULT_SNAPSHOT = STATE_DIR / "micro_pattern_dashboard_v1.json"
DEFAULT_DATABASE = STATE_DIR / "micro_pattern_quotes_v1.sqlite3"
SCHEMA_VERSION = 1
NANOSECONDS_PER_SECOND = 1_000_000_000
NANOSECONDS_PER_MINUTE = 60 * NANOSECONDS_PER_SECOND


@dataclass(frozen=True)
class MicroSpec:
    model_id: str
    mode: str
    order: int
    horizon_ms: int
    target: str = "fixed_ms"
    smoothing_tau_ms: int = 0
    equation_kind: str = "nlms"
    cluster_count: int = 0
    cross_signal: bool = False


MICRO_SPECS = (
    MicroSpec("micro.sign7.1000ms", "sign", 7, 1000),
    MicroSpec("micro.sign5.2000ms", "sign", 5, 2000),
    MicroSpec("micro.magnitude3.1000ms", "magnitude", 3, 1000),
    MicroSpec("micro.magnitude5.2000ms", "magnitude", 5, 2000),
    MicroSpec("intraminute.sign3.boundary", "sign", 3, 0, "minute_boundary"),
    MicroSpec("intraminute.sign5.boundary", "sign", 5, 0, "minute_boundary"),
    MicroSpec("intraminute.magnitude3.boundary", "magnitude", 3, 0, "minute_boundary"),
    MicroSpec("intraminute.smoothsign3.boundary", "smooth_sign", 3, 0, "minute_boundary", 400),
    MicroSpec("equation.ewls.1000ms", "equation", 0, 1000),
    MicroSpec("equation.ewls.2000ms", "equation", 0, 2000),
    MicroSpec("equation.ewls.5000ms", "equation", 0, 5000),
    MicroSpec("equation.smooth.1000ms", "equation", 0, 1000, "fixed_ms", 700),
    MicroSpec("equation.smooth.2000ms", "equation", 0, 2000, "fixed_ms", 1000),
    MicroSpec("intraminute.equation.boundary", "equation", 0, 0, "minute_boundary", 400),
    MicroSpec("intraminute.equation.smooth.boundary", "equation", 0, 0, "minute_boundary", 1000),
)

PRUNED_MICRO_SPECS = (
    MicroSpec("equation.huber.2000ms", "equation", 0, 2000, "fixed_ms", 400, "huber"),
    MicroSpec("intraminute.equation.huber.boundary", "equation", 0, 0, "minute_boundary", 400, "huber"),
    MicroSpec("equation.kmeans4.2000ms", "equation", 0, 2000, "fixed_ms", 400, "nlms", 4),
    MicroSpec("intraminute.equation.kmeans4.boundary", "equation", 0, 0, "minute_boundary", 400, "nlms", 4),
    MicroSpec("equation.cross_relative.2000ms", "equation", 0, 2000, "fixed_ms", 400, "nlms", 0, True),
    MicroSpec("intraminute.equation.cross_relative.boundary", "equation", 0, 0, "minute_boundary", 400, "nlms", 0, True),
)

EQUATION_FEATURES = (
    "bias",
    "velocity",
    "acceleration",
    "sum3",
    "sum5",
    "sum10",
    "mean_reversion3",
    "mean_reversion10",
    "volatility5",
    "volatility20",
    "spread",
    "spread_delta",
    "spread_velocity",
    "spread_ratio5",
    "activity_rate",
    "dt",
    "smooth_gap",
    "seconds_remaining",
)

CROSS_EQUATION_FEATURES = (
    "base_strength",
    "quote_strength",
    "relative_strength",
    "peer_agreement",
    "usd_factor",
)

CLUSTER_FEATURES = (
    "velocity",
    "volatility20",
    "spread_ratio5",
    "activity_rate",
    "smooth_gap",
    "seconds_remaining",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path: Path, payload: dict[str, Any], *, compressed: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = ".json.gz" if compressed else ".json"
    descriptor, temporary = tempfile.mkstemp(prefix=path.stem + ".", suffix=suffix, dir=path.parent)
    os.close(descriptor)
    temporary_path = Path(temporary)
    try:
        opener = gzip.open if compressed else open
        with opener(temporary_path, "wt", encoding="utf-8") as handle:
            json.dump(payload, handle, separators=(",", ":"), sort_keys=True)
        for attempt in range(6):
            try:
                os.replace(temporary_path, path)
                break
            except PermissionError:
                if attempt == 5:
                    raise
                time.sleep(0.05 * (attempt + 1))
    finally:
        temporary_path.unlink(missing_ok=True)


def read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    opener = gzip.open if path.suffix.lower() == ".gz" else open
    try:
        with opener(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def sign_label(code: int, order: int) -> str:
    return " ".join("U" if code & (1 << (order - 1 - index)) else "D" for index in range(order))


def magnitude_label(code: int, order: int) -> str:
    labels = ("D2", "D1", "F", "U1", "U2")
    digits = [0] * order
    for index in range(order - 1, -1, -1):
        digits[index] = code % 5
        code //= 5
    return " ".join(labels[digit] for digit in digits)


def target_time_ns(time_ns: int, spec: MicroSpec) -> int:
    if spec.target == "minute_boundary":
        return ((time_ns // NANOSECONDS_PER_MINUTE) + 1) * NANOSECONDS_PER_MINUTE
    return time_ns + spec.horizon_ms * 1_000_000


def target_horizon_ms(time_ns: int, target_ns: int) -> int:
    return max(1, int(round((target_ns - time_ns) / 1_000_000)))


def equation_feature_names(spec: MicroSpec) -> tuple[str, ...]:
    return EQUATION_FEATURES + CROSS_EQUATION_FEATURES if spec.cross_signal else EQUATION_FEATURES


def default_equation_state(feature_names: tuple[str, ...] = EQUATION_FEATURES) -> dict[str, Any]:
    return {
        "weights": {feature: 0.0 for feature in feature_names},
        "abs_baseline": empty_stats(),
        "learning_rate": 0.03,
    }


def equation_predict(
    features: dict[str, float],
    equation: dict[str, Any],
    feature_names: tuple[str, ...] = EQUATION_FEATURES,
) -> float:
    weights = equation.setdefault("weights", {feature: 0.0 for feature in feature_names})
    return sum(safe_float(weights.get(feature)) * safe_float(features.get(feature)) for feature in feature_names)


def update_equation(
    equation: dict[str, Any],
    features: dict[str, float],
    target: float,
    feature_names: tuple[str, ...] = EQUATION_FEATURES,
    equation_kind: str = "nlms",
) -> None:
    weights = equation.setdefault("weights", {feature: 0.0 for feature in feature_names})
    prediction = equation_predict(features, equation, feature_names)
    error = max(-10.0, min(10.0, safe_float(target) - prediction))
    if equation_kind == "huber":
        error = max(-1.5, min(1.5, error))
    norm = 1.0 + sum(safe_float(features.get(feature)) ** 2 for feature in feature_names)
    step = safe_float(equation.get("learning_rate") or 0.03) / norm
    for feature in feature_names:
        weights[feature] = safe_float(weights.get(feature)) + step * error * safe_float(features.get(feature))


def equation_definition(spec: MicroSpec) -> dict[str, Any]:
    if spec.mode != "equation":
        return {}
    target = (
        "signed mid-price move from prediction time to the next UTC minute boundary (variable 1-60000ms)"
        if spec.target == "minute_boundary"
        else f"signed mid-price move {spec.horizon_ms}ms after prediction"
    )
    if spec.cluster_count:
        fit_method = f"causal online k-means ({spec.cluster_count} regimes) with one normalized LMS equation per regime"
    elif spec.equation_kind == "huber":
        fit_method = "robust online normalized LMS with Huber-clipped residuals (delta 1.5 pips)"
    elif spec.cross_signal:
        fit_method = "cross-pair relative-strength online normalized LMS equation"
    else:
        fit_method = "online normalized least-mean-squares linear regression"
    notes = []
    if ".ewls." in spec.model_id:
        notes.append("legacy model id says ewls; implementation is normalized LMS without exponential sample forgetting")
    if spec.cross_signal:
        notes.append("cross features exclude the instrument being forecast and combine base/quote peer-pair moves")
    return {
        "fit_method": fit_method,
        "formula": "predicted_signed_pips = intercept + sum(weight_i * scaled_feature_i)",
        "target_definition": target,
        "horizon_label": "next minute boundary (1-60000ms)" if spec.target == "minute_boundary" else f"{spec.horizon_ms}ms",
        "features": list(equation_feature_names(spec)),
        "cluster_features": list(CLUSTER_FEATURES) if spec.cluster_count else [],
        "smoothing_tau_ms": spec.smoothing_tau_ms,
        "update_timing": "weights update only after the forecast target matures",
        "notes": notes,
    }


def empty_metrics() -> dict[str, float]:
    return {
        "predictions": 0.0,
        "ready_predictions": 0.0,
        "matured": 0.0,
        "ready_matured": 0.0,
        "correct": 0.0,
        "brier_sum": 0.0,
        "signed_mae_sum": 0.0,
        "absolute_mae_sum": 0.0,
        "net_pips_sum": 0.0,
        "coefficient_n": 0.0,
        "coefficient_x": 0.0,
        "coefficient_y": 0.0,
        "coefficient_xx": 0.0,
        "coefficient_yy": 0.0,
        "coefficient_xy": 0.0,
    }


def coefficient_correlation(metrics: dict[str, Any]) -> float | None:
    n = safe_float(metrics.get("coefficient_n"))
    if n < 2.0:
        return None
    sx = safe_float(metrics.get("coefficient_x"))
    sy = safe_float(metrics.get("coefficient_y"))
    sxx = safe_float(metrics.get("coefficient_xx"))
    syy = safe_float(metrics.get("coefficient_yy"))
    sxy = safe_float(metrics.get("coefficient_xy"))
    numerator = n * sxy - sx * sy
    denominator = math.sqrt(max(0.0, n * sxx - sx * sx) * max(0.0, n * syy - sy * sy))
    return None if denominator <= 0.0 else round(numerator / denominator, 4)


class MicroPatternModel:
    def __init__(
        self,
        database_path: Path,
        state_path: Path,
        snapshot_path: Path,
        pip_sizes: dict[str, float],
        *,
        prior_count: float = 20.0,
        min_pattern_count: int = 8,
        min_baseline_count: int = 20,
        retention_hours: float = 0.0,
        prune_interval_sec: float = 3600.0,
    ) -> None:
        self.database_path = database_path
        self.state_path = state_path
        self.snapshot_path = snapshot_path
        self.pip_sizes = pip_sizes
        self.prior_count = max(0.0, prior_count)
        self.min_pattern_count = max(1, min_pattern_count)
        self.min_baseline_count = max(1, min_baseline_count)
        self.retention_ns = max(0, int(retention_hours * 3600 * NANOSECONDS_PER_SECOND))
        self.prune_interval_sec = max(1.0, float(prune_interval_sec))
        state = read_json(state_path)
        if int(state.get("schema_version") or 0) != SCHEMA_VERSION:
            state = {}
        self.state: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "created_utc": state.get("created_utc") or utc_now(),
            "instruments": state.get("instruments") or {},
            "metrics": state.get("metrics") or {},
        }
        self.runtime: dict[str, dict[str, Any]] = {}
        self.latest: dict[str, dict[str, Any]] = {}
        self.recent_outcomes: deque[dict[str, Any]] = deque(maxlen=500)
        self.recent_signal_groups: deque[dict[str, float]] = deque(maxlen=500)
        self.received_updates = int(safe_float(state.get("received_updates")))
        self.changed_updates = int(safe_float(state.get("changed_updates")))
        self.started_monotonic = time.monotonic()
        self.last_commit = time.monotonic()
        self.last_state_save = 0.0
        self.last_prune = 0.0
        database_path.parent.mkdir(parents=True, exist_ok=True)
        self.database = sqlite3.connect(database_path)
        self.database.execute("PRAGMA journal_mode=WAL")
        self.database.execute("PRAGMA synchronous=NORMAL")
        self.database.executescript(
            """
            CREATE TABLE IF NOT EXISTS quotes (
                id INTEGER PRIMARY KEY,
                time_ns INTEGER NOT NULL,
                instrument TEXT NOT NULL,
                bid REAL NOT NULL,
                ask REAL NOT NULL,
                mid REAL NOT NULL,
                spread_pips REAL NOT NULL,
                tradeable INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS quotes_instrument_time ON quotes(instrument, time_ns);
            CREATE TABLE IF NOT EXISTS predictions (
                id INTEGER PRIMARY KEY,
                origin_time_ns INTEGER NOT NULL,
                target_time_ns INTEGER NOT NULL,
                instrument TEXT NOT NULL,
                model_id TEXT NOT NULL,
                pattern_code INTEGER NOT NULL,
                pattern TEXT NOT NULL,
                sequence_pips TEXT NOT NULL,
                horizon_ms INTEGER NOT NULL,
                probability_up REAL NOT NULL,
                predicted_direction TEXT NOT NULL,
                expected_signed_pips REAL NOT NULL,
                expected_abs_pips REAL NOT NULL,
                baseline_abs_pips REAL NOT NULL,
                movement_coefficient REAL NOT NULL,
                pattern_count INTEGER NOT NULL,
                baseline_count INTEGER NOT NULL,
                model_ready INTEGER NOT NULL,
                entry_bid REAL NOT NULL,
                entry_ask REAL NOT NULL,
                status TEXT NOT NULL,
                outcome_time_ns INTEGER,
                actual_signed_pips REAL,
                actual_abs_pips REAL,
                actual_movement_coefficient REAL,
                direction_correct INTEGER,
                probability_brier_score REAL,
                theoretical_pips REAL
            );
            CREATE INDEX IF NOT EXISTS predictions_model_time ON predictions(model_id, origin_time_ns);
            CREATE INDEX IF NOT EXISTS predictions_instrument_time ON predictions(instrument, origin_time_ns);
            """
        )
        self.database.commit()

    def prune_history(
        self,
        *,
        force: bool = False,
        now_ns: int | None = None,
    ) -> dict[str, Any]:
        """Bound raw rows while cumulative model statistics remain in state."""

        if self.retention_ns <= 0:
            return {"status": "disabled", "quotes_deleted": 0, "predictions_deleted": 0}
        monotonic_now = time.monotonic()
        if not force and monotonic_now - self.last_prune < self.prune_interval_sec:
            return {"status": "not_due", "quotes_deleted": 0, "predictions_deleted": 0}
        cutoff_ns = int(now_ns if now_ns is not None else time.time_ns()) - self.retention_ns
        quotes_deleted = 0
        instruments = set(self.pip_sizes)
        instruments.update(
            str(row[0])
            for row in self.database.execute("SELECT DISTINCT instrument FROM quotes")
        )
        for instrument in instruments:
            cursor = self.database.execute(
                "DELETE FROM quotes WHERE instrument=? AND time_ns<?",
                (instrument, cutoff_ns),
            )
            quotes_deleted += max(0, int(cursor.rowcount))
        predictions_deleted = 0
        model_ids = {
            *(spec.model_id for spec in MICRO_SPECS),
            *(spec.model_id for spec in PRUNED_MICRO_SPECS),
        }
        model_ids.update(
            str(row[0])
            for row in self.database.execute("SELECT DISTINCT model_id FROM predictions")
        )
        for model_id in model_ids:
            cursor = self.database.execute(
                "DELETE FROM predictions WHERE model_id=? AND origin_time_ns<?",
                (model_id, cutoff_ns),
            )
            predictions_deleted += max(0, int(cursor.rowcount))
        self.last_prune = monotonic_now
        result = {
            "status": "pruned",
            "cutoff_time_ns": cutoff_ns,
            "retention_hours": self.retention_ns / NANOSECONDS_PER_SECOND / 3600.0,
            "quotes_deleted": quotes_deleted,
            "predictions_deleted": predictions_deleted,
            "pruned_utc": utc_now(),
        }
        self.state["storage_retention"] = result
        return result

    def _instrument_state(self, instrument: str) -> dict[str, Any]:
        instruments = self.state["instruments"]
        row = instruments.setdefault(instrument, {"models": {}})
        row.setdefault("models", {})
        return row

    def _model_state(self, instrument: str, spec: MicroSpec) -> dict[str, Any]:
        models = self._instrument_state(instrument)["models"]
        row = models.setdefault(spec.model_id, {"baseline": empty_stats(), "patterns": {}})
        row.setdefault("baseline", empty_stats())
        row.setdefault("patterns", {})
        if spec.mode == "equation":
            row.setdefault("equation", default_equation_state(equation_feature_names(spec)))
            if spec.cluster_count:
                row.setdefault("cluster_equations", [])
        return row

    def _metrics(self, model_id: str) -> dict[str, Any]:
        row = self.state["metrics"].setdefault(model_id, empty_metrics())
        for key, value in empty_metrics().items():
            row.setdefault(key, value)
        return row

    def _runtime(self, instrument: str) -> dict[str, Any]:
        row = self.runtime.setdefault(
            instrument,
            {
                "last_mid": None,
                "smooth_mid": None,
                "last_spread_pips": None,
                "last_time_ns": 0,
                "moves": deque(maxlen=128),
                "pending": deque(),
            },
        )
        row["instrument"] = instrument
        return row

    def _pattern(self, runtime: dict[str, Any], spec: MicroSpec) -> tuple[int, str, list[float]] | None:
        if spec.mode == "equation":
            return -1, "continuous equation", []
        moves = list(runtime["moves"])
        if len(moves) < spec.order:
            return None
        selected = moves[-spec.order :]
        if spec.mode in {"sign", "smooth_sign"}:
            code = 0
            for move in selected:
                key = "smoothed_pips" if spec.mode == "smooth_sign" else "pips"
                code = code * 2 + int(safe_float(move[key]) > 0.0)
            label = sign_label(code, spec.order)
        else:
            categories = [move.get("magnitude") for move in selected]
            if any(category is None for category in categories):
                return None
            code = 0
            for category in categories:
                code = code * 5 + int(category)
            label = magnitude_label(code, spec.order)
        sequence_key = "smoothed_pips" if spec.mode == "smooth_sign" else "pips"
        return code, label, [round(safe_float(move[sequence_key]), 4) for move in selected]

    def _equation_features(
        self,
        runtime: dict[str, Any],
        spec: MicroSpec,
        time_ns: int,
        spread_pips: float,
    ) -> dict[str, float] | None:
        moves = list(runtime["moves"])
        if len(moves) < 5:
            return None
        last = moves[-1]
        previous = moves[-2]
        dt_sec = max(0.001, safe_float(last.get("dt_ms")) / 1000.0)
        last_delta = safe_float(last.get("smoothed_pips") if spec.smoothing_tau_ms else last.get("pips"))
        previous_delta = safe_float(previous.get("smoothed_pips") if spec.smoothing_tau_ms else previous.get("pips"))
        selected_key = "smoothed_pips" if spec.smoothing_tau_ms else "pips"
        sum3 = sum(safe_float(move.get(selected_key)) for move in moves[-3:])
        sum5 = sum(safe_float(move.get(selected_key)) for move in moves[-5:])
        sum10 = sum(safe_float(move.get(selected_key)) for move in moves[-10:])
        abs5 = [abs(safe_float(move.get(selected_key))) for move in moves[-5:]]
        abs20 = [abs(safe_float(move.get(selected_key))) for move in moves[-20:]]
        spread_values = [safe_float(move.get("spread_pips")) for move in moves[-6:] if safe_float(move.get("spread_pips")) > 0.0]
        spread_baseline = statistics.median(spread_values[:-1]) if len(spread_values) >= 2 else max(0.001, spread_pips)
        spread_delta = safe_float(last.get("spread_delta_pips"))
        spread_velocity = safe_float(last.get("spread_velocity_pips_sec"))
        activity_rate = 1.0 / dt_sec
        target_ns = target_time_ns(time_ns, spec)
        seconds_remaining = max(0.001, (target_ns - time_ns) / NANOSECONDS_PER_SECOND)
        scale = max(1.0, seconds_remaining)
        features = {
            "bias": 1.0,
            "velocity": max(-5.0, min(5.0, last_delta / dt_sec)) / 5.0,
            "acceleration": max(-5.0, min(5.0, (last_delta - previous_delta) / dt_sec)) / 5.0,
            "sum3": max(-10.0, min(10.0, sum3)) / 10.0,
            "sum5": max(-15.0, min(15.0, sum5)) / 15.0,
            "sum10": max(-20.0, min(20.0, sum10)) / 20.0,
            "mean_reversion3": max(-10.0, min(10.0, -sum3)) / 10.0,
            "mean_reversion10": max(-20.0, min(20.0, -sum10)) / 20.0,
            "volatility5": max(0.0, min(5.0, statistics.fmean(abs5))) / 5.0,
            "volatility20": max(0.0, min(5.0, statistics.fmean(abs20))) / 5.0,
            "spread": min(5.0, max(0.0, spread_pips)) / 5.0,
            "spread_delta": max(-3.0, min(3.0, spread_delta)) / 3.0,
            "spread_velocity": max(-5.0, min(5.0, spread_velocity)) / 5.0,
            "spread_ratio5": min(3.0, max(0.0, spread_pips / max(0.001, spread_baseline))) / 3.0,
            "activity_rate": min(20.0, max(0.0, activity_rate)) / 20.0,
            "dt": min(5.0, max(0.0, dt_sec)) / 5.0,
            "smooth_gap": max(-5.0, min(5.0, safe_float(last.get("smooth_gap_pips")))) / 5.0,
            "seconds_remaining": min(60.0, scale) / 60.0,
        }
        if spec.cross_signal:
            features.update(self._cross_equation_features(runtime.get("instrument") or "", time_ns))
        return features

    def _cross_equation_features(self, instrument: str, time_ns: int) -> dict[str, float]:
        strengths: dict[str, list[float]] = {}
        peer_signs: list[float] = []
        base, _, quote = instrument.partition("_")
        for peer_instrument, peer_runtime in self.runtime.items():
            if peer_instrument == instrument or time_ns - int(peer_runtime.get("last_time_ns") or 0) > 2 * NANOSECONDS_PER_SECOND:
                continue
            moves = peer_runtime.get("moves") or []
            if not moves or "_" not in peer_instrument:
                continue
            last = moves[-1]
            velocity = max(-1.0, min(1.0, safe_float(last.get("pips")) / max(0.001, safe_float(last.get("dt_ms")) / 1000.0) / 5.0))
            peer_base, peer_quote = peer_instrument.split("_", 1)
            strengths.setdefault(peer_base, []).append(velocity)
            strengths.setdefault(peer_quote, []).append(-velocity)
            if base in (peer_base, peer_quote) or quote in (peer_base, peer_quote):
                peer_signs.append(velocity if peer_base in (base, quote) else -velocity)
        currency_strength = {
            currency: statistics.fmean(values)
            for currency, values in strengths.items()
            if values
        }
        base_strength = safe_float(currency_strength.get(base))
        quote_strength = safe_float(currency_strength.get(quote))
        relative = max(-1.0, min(1.0, base_strength - quote_strength))
        agreement = statistics.fmean(1.0 if value * relative > 0.0 else -1.0 for value in peer_signs) if peer_signs and relative else 0.0
        return {
            "base_strength": base_strength,
            "quote_strength": quote_strength,
            "relative_strength": relative,
            "peer_agreement": agreement,
            "usd_factor": safe_float(currency_strength.get("USD")),
        }

    def _cluster_equation(
        self,
        model: dict[str, Any],
        spec: MicroSpec,
        features: dict[str, float],
        cluster_id: int | None = None,
    ) -> tuple[dict[str, Any], int]:
        clusters = model.setdefault("cluster_equations", [])
        vector = [safe_float(features.get(feature)) for feature in CLUSTER_FEATURES]
        if cluster_id is None:
            if len(clusters) < spec.cluster_count:
                clusters.append(
                    {
                        "centroid": vector,
                        "count": 1,
                        "equation": default_equation_state(equation_feature_names(spec)),
                    }
                )
                return clusters[-1]["equation"], len(clusters) - 1
            cluster_id = min(
                range(len(clusters)),
                key=lambda index: sum(
                    (value - safe_float(clusters[index]["centroid"][offset])) ** 2
                    for offset, value in enumerate(vector)
                ),
            )
            cluster = clusters[cluster_id]
            count = int(safe_float(cluster.get("count"))) + 1
            rate = min(0.05, 1.0 / max(1, count))
            cluster["centroid"] = [
                safe_float(old) + rate * (value - safe_float(old))
                for old, value in zip(cluster.get("centroid") or vector, vector)
            ]
            cluster["count"] = count
        cluster_id = max(0, min(int(cluster_id), len(clusters) - 1))
        equation = clusters[cluster_id].setdefault("equation", default_equation_state(equation_feature_names(spec)))
        return equation, cluster_id

    def _forecast(
        self,
        instrument: str,
        spec: MicroSpec,
        code: int,
        label: str,
        sequence: list[float],
        time_ns: int,
        target_ns: int,
        equation_features: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        model = self._model_state(instrument, spec)
        baseline = model["baseline"]
        pattern = model["patterns"].get(str(code)) or {}
        baseline_count = int(safe_float(baseline.get("count")))
        pattern_count = int(safe_float(pattern.get("count")))
        baseline_probability = stats_rate(baseline, "probability_up") if baseline_count else 0.5
        baseline_mean = stats_rate(baseline, "mean") if baseline_count else 0.0
        baseline_abs = stats_rate(baseline, "mean_abs") if baseline_count else 0.0
        effective = pattern_count + self.prior_count
        probability_up = (
            stats_rate(pattern, "probability_up") * pattern_count + baseline_probability * self.prior_count
        ) / max(effective, 1.0)
        expected_signed = (
            stats_rate(pattern, "mean") * pattern_count + baseline_mean * self.prior_count
        ) / max(effective, 1.0)
        expected_abs = (
            stats_rate(pattern, "mean_abs") * pattern_count + baseline_abs * self.prior_count
        ) / max(effective, 1.0)
        equation_cluster_id: int | None = None
        if spec.mode == "equation":
            feature_names = equation_feature_names(spec)
            if spec.cluster_count:
                equation, equation_cluster_id = self._cluster_equation(model, spec, equation_features or {})
            else:
                equation = model.setdefault("equation", default_equation_state(feature_names))
            predicted = equation_predict(equation_features or {}, equation, feature_names)
            equation_abs_baseline = equation.setdefault("abs_baseline", empty_stats())
            equation_abs = stats_rate(equation_abs_baseline, "mean_abs") if equation_abs_baseline.get("count") else baseline_abs
            expected_signed = predicted
            expected_abs = max(abs(predicted), equation_abs)
            probability_up = 1.0 / (1.0 + math.exp(-max(-8.0, min(8.0, predicted / max(equation_abs, 0.05)))))
            pattern_count = int(safe_float(equation_abs_baseline.get("count")))
            label = "continuous equation" + (f" / regime {equation_cluster_id + 1}" if equation_cluster_id is not None else "")
            sequence = [round(safe_float((equation_features or {}).get(feature)), 5) for feature in feature_names]
        coefficient = expected_abs / baseline_abs if baseline_abs > 1e-12 else 1.0
        direction = "buy" if probability_up >= 0.5 else "sell"
        ready = pattern_count >= self.min_pattern_count and baseline_count >= self.min_baseline_count
        horizon_ms = target_horizon_ms(time_ns, target_ns)
        return {
            "model_id": spec.model_id,
            "model": "live_equation_forecast" if spec.mode == "equation" else "live_micro_pattern",
            "instrument": instrument,
            "pattern_mode": spec.mode,
            "pattern_order": spec.order,
            "pattern_code": code,
            "pattern": label,
            "sequence_pips": sequence,
            "origin_time_ns": time_ns,
            "origin_time": datetime.fromtimestamp(time_ns / 1_000_000_000, timezone.utc).isoformat(),
            "target_time_ns": target_ns,
            "target_time": datetime.fromtimestamp(target_ns / 1_000_000_000, timezone.utc).isoformat(),
            "target_horizon_ms": horizon_ms,
            "target_kind": spec.target,
            "probability_up": round(probability_up, 6),
            "probability_down": round(1.0 - probability_up, 6),
            "predicted_direction": direction,
            "direction_edge": round(abs(probability_up - 0.5), 6),
            "expected_signed_move_pips": round(expected_signed, 5),
            "expected_abs_move_pips": round(expected_abs, 5),
            "baseline_abs_move_pips": round(baseline_abs, 5),
            "movement_coefficient": round(coefficient, 6),
            "coefficient_of_movement": round(coefficient, 6),
            "live_pattern_count": pattern_count,
            "live_baseline_count": baseline_count,
            "model_ready": ready,
            "equation_features": equation_features or {},
            "equation_cluster_id": equation_cluster_id,
            "equation_definition": equation_definition(spec),
            "smoothing_tau_ms": spec.smoothing_tau_ms,
            "data_sources": ["oanda_live_price_stream"],
            "resolution_note": "OANDA sampled pricing, up to 4 updates/sec/instrument",
        }

    def _insert_prediction(self, forecast: dict[str, Any], target_ns: int, bid: float, ask: float) -> int:
        cursor = self.database.execute(
            """
            INSERT INTO predictions (
                origin_time_ns,target_time_ns,instrument,model_id,pattern_code,pattern,sequence_pips,
                horizon_ms,probability_up,predicted_direction,expected_signed_pips,expected_abs_pips,
                baseline_abs_pips,movement_coefficient,pattern_count,baseline_count,model_ready,
                entry_bid,entry_ask,status
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending')
            """,
            (
                forecast["origin_time_ns"], target_ns, forecast["instrument"], forecast["model_id"],
                forecast["pattern_code"], forecast["pattern"], json.dumps(forecast["sequence_pips"]),
                forecast["target_horizon_ms"], forecast["probability_up"], forecast["predicted_direction"],
                forecast["expected_signed_move_pips"], forecast["expected_abs_move_pips"],
                forecast["baseline_abs_move_pips"], forecast["movement_coefficient"],
                forecast["live_pattern_count"], forecast["live_baseline_count"], int(forecast["model_ready"]),
                bid, ask,
            ),
        )
        return int(cursor.lastrowid)

    def _mature(self, instrument: str, runtime: dict[str, Any], time_ns: int, bid: float, ask: float) -> None:
        remaining: deque[dict[str, Any]] = deque()
        mid = (bid + ask) / 2.0
        pip = self.pip_sizes[instrument]
        while runtime["pending"]:
            pending = runtime["pending"].popleft()
            if time_ns < pending["target_ns"]:
                remaining.append(pending)
                continue
            forecast = pending["forecast"]
            actual_signed = (mid - pending["entry_mid"]) / pip
            actual_abs = abs(actual_signed)
            spec = pending["spec"]
            model = self._model_state(instrument, spec)
            add_observation(model["baseline"], actual_signed)
            if spec.mode == "equation":
                feature_names = equation_feature_names(spec)
                if spec.cluster_count:
                    equation, _ = self._cluster_equation(
                        model,
                        spec,
                        forecast.get("equation_features") or {},
                        int(safe_float(forecast.get("equation_cluster_id"))),
                    )
                else:
                    equation = model.setdefault("equation", default_equation_state(feature_names))
                add_observation(equation.setdefault("abs_baseline", empty_stats()), actual_signed)
                update_equation(
                    equation,
                    forecast.get("equation_features") or {},
                    actual_signed,
                    feature_names,
                    spec.equation_kind,
                )
            else:
                pattern_stats = model["patterns"].setdefault(str(forecast["pattern_code"]), empty_stats())
                add_observation(pattern_stats, actual_signed)
            baseline_abs = max(1e-12, safe_float(forecast["baseline_abs_move_pips"]))
            actual_coefficient = actual_abs / baseline_abs if baseline_abs > 1e-12 else 0.0
            direction_correct = (
                (forecast["predicted_direction"] == "buy" and actual_signed > 0.0)
                or (forecast["predicted_direction"] == "sell" and actual_signed < 0.0)
            )
            actual_up = 1.0 if actual_signed > 0.0 else 0.0
            brier = (safe_float(forecast["probability_up"]) - actual_up) ** 2
            theoretical = (
                (bid - pending["entry_ask"]) / pip
                if forecast["predicted_direction"] == "buy"
                else (pending["entry_bid"] - ask) / pip
            )
            outcome = {
                "prediction_id": pending["prediction_id"],
                "model_id": forecast["model_id"],
                "instrument": instrument,
                "origin_time": forecast.get("origin_time"),
                "origin_time_ns": forecast.get("origin_time_ns"),
                "target_time": forecast.get("target_time"),
                "target_time_ns": forecast.get("target_time_ns"),
                "target_horizon_ms": forecast.get("target_horizon_ms"),
                "predicted_direction": forecast.get("predicted_direction"),
                "probability_up": forecast.get("probability_up"),
                "expected_signed_move_pips": forecast.get("expected_signed_move_pips"),
                "expected_abs_move_pips": forecast.get("expected_abs_move_pips"),
                "baseline_abs_move_pips": forecast.get("baseline_abs_move_pips"),
                "movement_coefficient": forecast.get("movement_coefficient"),
                "live_pattern_count": forecast.get("live_pattern_count"),
                "live_baseline_count": forecast.get("live_baseline_count"),
                "model_ready": forecast.get("model_ready"),
                "outcome_time_ns": time_ns,
                "actual_signed_move_pips": round(actual_signed, 5),
                "actual_abs_move_pips": round(actual_abs, 5),
                "actual_movement_coefficient": round(actual_coefficient, 6),
                "direction_correct": direction_correct,
                "probability_brier_score": round(brier, 6),
                "theoretical_pips": round(theoretical, 5),
            }
            self.database.execute(
                """
                UPDATE predictions SET status='matured',outcome_time_ns=?,actual_signed_pips=?,actual_abs_pips=?,
                    actual_movement_coefficient=?,direction_correct=?,probability_brier_score=?,theoretical_pips=?
                WHERE id=?
                """,
                (
                    time_ns, actual_signed, actual_abs, actual_coefficient, int(direction_correct), brier,
                    theoretical, pending["prediction_id"],
                ),
            )
            metrics = self._metrics(forecast["model_id"])
            metrics["matured"] = safe_float(metrics["matured"]) + 1.0
            if forecast["model_ready"]:
                metrics["ready_matured"] = safe_float(metrics["ready_matured"]) + 1.0
                metrics["correct"] = safe_float(metrics["correct"]) + float(direction_correct)
                metrics["brier_sum"] = safe_float(metrics["brier_sum"]) + brier
                metrics["signed_mae_sum"] = safe_float(metrics["signed_mae_sum"]) + abs(
                    actual_signed - safe_float(forecast["expected_signed_move_pips"])
                )
                metrics["absolute_mae_sum"] = safe_float(metrics["absolute_mae_sum"]) + abs(
                    actual_abs - safe_float(forecast["expected_abs_move_pips"])
                )
                metrics["net_pips_sum"] = safe_float(metrics["net_pips_sum"]) + theoretical
                predicted_coefficient = safe_float(forecast["movement_coefficient"])
                metrics["coefficient_n"] = safe_float(metrics["coefficient_n"]) + 1.0
                metrics["coefficient_x"] = safe_float(metrics["coefficient_x"]) + predicted_coefficient
                metrics["coefficient_y"] = safe_float(metrics["coefficient_y"]) + actual_coefficient
                metrics["coefficient_xx"] = safe_float(metrics["coefficient_xx"]) + predicted_coefficient**2
                metrics["coefficient_yy"] = safe_float(metrics["coefficient_yy"]) + actual_coefficient**2
                metrics["coefficient_xy"] = safe_float(metrics["coefficient_xy"]) + predicted_coefficient * actual_coefficient
            latest_key = f"{instrument}:{forecast['model_id']}"
            if safe_float((self.latest.get(latest_key) or {}).get("prediction_id")) == pending["prediction_id"]:
                self.latest[latest_key]["outcome"] = outcome
            self.recent_outcomes.append(outcome)
        runtime["pending"] = remaining

    def observe(
        self,
        instrument: str,
        time_value: str,
        bid: float,
        ask: float,
        tradeable: bool = True,
    ) -> bool:
        if instrument not in self.pip_sizes or bid <= 0.0 or ask <= bid:
            return False
        time_ns = parse_time_ns(time_value) or time.time_ns()
        runtime = self._runtime(instrument)
        previous_time_ns = int(runtime["last_time_ns"])
        if time_ns <= previous_time_ns:
            return False
        mid = (bid + ask) / 2.0
        pip = self.pip_sizes[instrument]
        self.received_updates += 1
        self.database.execute(
            "INSERT INTO quotes(time_ns,instrument,bid,ask,mid,spread_pips,tradeable) VALUES(?,?,?,?,?,?,?)",
            (time_ns, instrument, bid, ask, mid, (ask - bid) / pip, int(tradeable)),
        )
        self._mature(instrument, runtime, time_ns, bid, ask)
        previous_mid = runtime["last_mid"]
        previous_smooth_mid = runtime["smooth_mid"]
        previous_spread_pips = runtime.get("last_spread_pips")
        runtime["last_time_ns"] = time_ns
        runtime["last_mid"] = mid
        spread_pips = (ask - bid) / pip
        runtime["last_spread_pips"] = spread_pips
        if previous_smooth_mid is None:
            runtime["smooth_mid"] = mid
        if previous_mid is None or math.isclose(mid, previous_mid, abs_tol=pip * 1e-6):
            self._maybe_commit()
            return True

        dt_ms = max(1.0, (time_ns - previous_time_ns) / 1_000_000)
        alpha = 1.0 - math.exp(-dt_ms / 400.0)
        smooth_mid = mid if previous_smooth_mid is None else previous_smooth_mid + alpha * (mid - previous_smooth_mid)
        runtime["smooth_mid"] = smooth_mid
        delta_pips = (mid - previous_mid) / pip
        smoothed_delta_pips = (
            delta_pips if previous_smooth_mid is None else (smooth_mid - previous_smooth_mid) / pip
        )
        prior_moves = list(runtime["moves"])[-60:]
        magnitude: int | None = None
        if len(prior_moves) >= 10:
            scale = statistics.median(abs(safe_float(move["pips"])) for move in prior_moves)
            if scale > 0.0:
                ratio = delta_pips / scale
                magnitude = 0 if ratio <= -1.5 else 1 if ratio < -0.1 else 2 if ratio <= 0.1 else 3 if ratio < 1.5 else 4
        spread_delta_pips = 0.0 if previous_spread_pips is None else spread_pips - safe_float(previous_spread_pips)
        runtime["moves"].append(
            {
                "pips": delta_pips,
                "smoothed_pips": smoothed_delta_pips,
                "dt_ms": dt_ms,
                "magnitude": magnitude,
                "spread_pips": spread_pips,
                "spread_delta_pips": spread_delta_pips,
                "spread_velocity_pips_sec": spread_delta_pips / max(0.001, dt_ms / 1000.0),
                "smooth_gap_pips": (mid - smooth_mid) / pip,
            }
        )
        self.changed_updates += 1
        equation_signal_groups: dict[int, dict[str, float]] = {}
        for spec in MICRO_SPECS:
            pattern = self._pattern(runtime, spec)
            if pattern is None:
                continue
            code, label, sequence = pattern
            target_ns = target_time_ns(time_ns, spec)
            if target_ns <= time_ns:
                continue
            equation_features = None
            if spec.mode == "equation":
                equation_features = self._equation_features(runtime, spec, time_ns, spread_pips)
                if equation_features is None:
                    continue
            forecast = self._forecast(
                instrument,
                spec,
                code,
                label,
                sequence,
                time_ns,
                target_ns,
                equation_features,
            )
            prediction_id = self._insert_prediction(forecast, target_ns, bid, ask)
            forecast["prediction_id"] = prediction_id
            forecast["entry_bid"] = bid
            forecast["entry_ask"] = ask
            forecast["spread_pips"] = round(spread_pips, 5)
            self.latest[f"{instrument}:{spec.model_id}"] = forecast
            if spec.mode == "equation":
                equation_signal_groups.setdefault(target_ns, {})[spec.model_id] = safe_float(
                    forecast.get("expected_signed_move_pips")
                )
            metrics = self._metrics(spec.model_id)
            metrics["predictions"] = safe_float(metrics["predictions"]) + 1.0
            if forecast["model_ready"]:
                metrics["ready_predictions"] = safe_float(metrics["ready_predictions"]) + 1.0
            runtime["pending"].append(
                {
                    "spec": spec,
                    "target_ns": target_ns,
                    "entry_mid": mid,
                    "entry_bid": bid,
                    "entry_ask": ask,
                    "prediction_id": prediction_id,
                    "forecast": forecast,
                }
            )
        for group in equation_signal_groups.values():
            if len(group) >= 2:
                self.recent_signal_groups.append(group)
        self._maybe_commit()
        return True

    def _maybe_commit(self) -> None:
        if time.monotonic() - self.last_commit >= 1.0:
            self.database.commit()
            self.last_commit = time.monotonic()

    def summaries(self, specs: tuple[MicroSpec, ...] = MICRO_SPECS) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for spec in specs:
            metrics = self._metrics(spec.model_id)
            n = safe_float(metrics["ready_matured"])
            output.append(
                {
                    **asdict(spec),
                    **equation_definition(spec),
                    "predictions": int(safe_float(metrics["predictions"])),
                    "ready_predictions": int(safe_float(metrics["ready_predictions"])),
                    "matured": int(safe_float(metrics["matured"])),
                    "ready_matured": int(n),
                    "direction_accuracy": round(100.0 * safe_float(metrics["correct"]) / n, 2) if n else 0.0,
                    "mean_brier_score": round(safe_float(metrics["brier_sum"]) / n, 5) if n else 0.0,
                    "signed_move_mae_pips": round(safe_float(metrics["signed_mae_sum"]) / n, 5) if n else 0.0,
                    "absolute_move_mae_pips": round(safe_float(metrics["absolute_mae_sum"]) / n, 5) if n else 0.0,
                    "average_net_pips": round(safe_float(metrics["net_pips_sum"]) / n, 5) if n else 0.0,
                    "movement_coefficient_correlation": coefficient_correlation(metrics),
                }
            )
        return output

    def signal_correlation_matrix(self) -> dict[str, Any]:
        pair_values: dict[tuple[str, str], list[tuple[float, float]]] = {}
        for group in self.recent_signal_groups:
            models = sorted(group)
            for left_index, left in enumerate(models):
                for right in models[left_index + 1 :]:
                    pair_values.setdefault((left, right), []).append((group[left], group[right]))
        cells = []
        for (left, right), values in pair_values.items():
            if len(values) < 10:
                continue
            xs = [value[0] for value in values]
            ys = [value[1] for value in values]
            mean_x = statistics.fmean(xs)
            mean_y = statistics.fmean(ys)
            covariance = sum((x - mean_x) * (y - mean_y) for x, y in values)
            variance_x = sum((x - mean_x) ** 2 for x in xs)
            variance_y = sum((y - mean_y) ** 2 for y in ys)
            correlation = covariance / math.sqrt(variance_x * variance_y) if variance_x > 0.0 and variance_y > 0.0 else None
            agreement = 100.0 * sum(1 for x, y in values if (x >= 0.0) == (y >= 0.0)) / len(values)
            cells.append(
                {
                    "left": left,
                    "right": right,
                    "n": len(values),
                    "correlation": round(correlation, 4) if correlation is not None else None,
                    "direction_agreement": round(agreement, 2),
                }
            )
        cells.sort(key=lambda row: (-int(row["n"]), -abs(safe_float(row.get("correlation")))))
        return {"group_count": len(self.recent_signal_groups), "cells": cells[:120]}

    def ruleset_leaderboard(self, summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = [
            row
            for row in summaries
            if row.get("mode") == "equation"
            and (row.get("equation_kind") != "nlms" or row.get("cluster_count") or row.get("cross_signal"))
        ]
        for row in rows:
            n = int(safe_float(row.get("ready_matured")))
            row["evidence_score"] = round(
                safe_float(row.get("average_net_pips")) * min(1.0, n / 1000.0),
                6,
            )
            row["deployment_status"] = "shadow only"
        return sorted(rows, key=lambda row: (-safe_float(row.get("evidence_score")), -int(safe_float(row.get("ready_matured")))))

    def save_snapshot(self, *, active: bool = True, stream: dict[str, Any] | None = None) -> None:
        summaries = self.summaries()
        retired = self.summaries(PRUNED_MICRO_SPECS)
        for row in retired:
            row["deployment_status"] = "retired shadow variant"
            row["retired_reason"] = "More than 100,000 matured forecasts remained negative after spread with below-50% directional accuracy."
        payload = {
            "schema_version": SCHEMA_VERSION,
            "time": utc_now(),
            "active": active,
            "resolution": "OANDA sampled pricing, up to 4 updates/sec/instrument",
            "historical_subsecond_data_used": False,
            "received_quote_updates": self.received_updates,
            "changed_quote_updates": self.changed_updates,
            "tracked_instruments": len(self.runtime),
            "database": str(self.database_path.resolve()),
            "state": str(self.state_path.resolve()),
            "stream": stream or {},
            "models": summaries,
            "retired_models": retired,
            "signal_correlation_matrix": self.signal_correlation_matrix(),
            "ruleset_leaderboard": self.ruleset_leaderboard(summaries),
            "latest_forecasts": sorted(
                self.latest.values(), key=lambda row: (str(row["model_id"]), str(row["instrument"]))
            ),
            "recent_outcomes": list(self.recent_outcomes),
        }
        atomic_json(self.snapshot_path, payload)

    def save_state(self) -> None:
        self.prune_history()
        self.database.commit()
        self.state.update(
            {
                "updated_utc": utc_now(),
                "received_updates": self.received_updates,
                "changed_updates": self.changed_updates,
                "specs": [asdict(spec) for spec in MICRO_SPECS],
                "pruned_specs": [asdict(spec) for spec in PRUNED_MICRO_SPECS],
            }
        )
        atomic_json(self.state_path, self.state, compressed=True)
        self.last_state_save = time.monotonic()

    def close(self) -> None:
        self.save_state()
        self.save_snapshot(active=False)
        self.database.close()


def log_line(path: Path, event: str, **fields: Any) -> None:
    payload = {"time": utc_now(), "event": event, **fields}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, default=str) + "\n")


def account_pip_sizes(token: str, account_id: str) -> dict[str, float]:
    response = requests.get(
        f"https://api-fxpractice.oanda.com/v3/accounts/{account_id}/instruments",
        headers={"Authorization": f"Bearer {token}"},
        timeout=20,
    )
    response.raise_for_status()
    output: dict[str, float] = {}
    for row in response.json().get("instruments") or []:
        name = str(row.get("name") or "")
        try:
            pip = 10.0 ** int(row.get("pipLocation"))
        except (TypeError, ValueError):
            continue
        if name and 0.0 < pip <= 1.0:
            output[name] = pip
    return output


def run(args: argparse.Namespace) -> int:
    token, account_id = read_credentials(args.creds, args.account_key, args.account_id)
    instruments = tradeable_currency_instruments(token, account_id)
    pip_sizes = account_pip_sizes(token, account_id)
    instruments = [instrument for instrument in instruments if instrument in pip_sizes]
    args.log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    log_path = args.log_dir / f"practice_micro_pattern_{stamp}.jsonl"
    model = MicroPatternModel(
        args.database,
        args.state,
        args.snapshot,
        pip_sizes,
        prior_count=args.prior_count,
        min_pattern_count=args.min_pattern_count,
        min_baseline_count=args.min_baseline_count,
        retention_hours=args.retention_hours,
        prune_interval_sec=args.prune_interval_sec,
    )
    log_line(
        log_path,
        "micro_lab_start",
        account_suffix=account_id[-4:],
        instrument_count=len(instruments),
        models=[asdict(spec) for spec in MICRO_SPECS],
        database=str(args.database.resolve()),
        retention_hours=args.retention_hours,
        places_orders=False,
        historical_subsecond_data_used=False,
    )
    stop_at = time.monotonic() + args.duration_sec
    failures = 0
    last_snapshot = 0.0
    last_tick = 0.0
    response: requests.Response | None = None
    try:
        while time.monotonic() < stop_at:
            try:
                token, account_id = read_credentials(args.creds, args.account_key, args.account_id)
                response = requests.get(
                    f"{STREAM_URL}/v3/accounts/{account_id}/pricing/stream",
                    headers={"Authorization": f"Bearer {token}", "Accept-Datetime-Format": "RFC3339"},
                    params={
                        "instruments": ",".join(instruments),
                        "snapshot": "true",
                        "includeHomeConversions": "false",
                    },
                    stream=True,
                    timeout=(10, 15),
                )
                if response.status_code >= 400:
                    failures += 1
                    log_line(log_path, "micro_stream_http_error", status=response.status_code)
                    response.close()
                    time.sleep(min(15.0, 0.5 * 2 ** min(failures, 5)))
                    continue
                failures = 0
                log_line(log_path, "micro_stream_connected")
                for raw_line in response.iter_lines():
                    if time.monotonic() >= stop_at:
                        break
                    if not raw_line:
                        continue
                    try:
                        payload = json.loads(raw_line.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        continue
                    payload_type = str(payload.get("type") or "").upper()
                    now = time.monotonic()
                    if payload_type != "HEARTBEAT":
                        bids = payload.get("bids") or []
                        asks = payload.get("asks") or []
                        instrument = str(payload.get("instrument") or "")
                        if instrument in pip_sizes and bids and asks:
                            model.observe(
                                instrument,
                                str(payload.get("time") or ""),
                                safe_float(bids[0].get("price")),
                                safe_float(asks[0].get("price")),
                                bool(payload.get("tradeable", True)),
                            )
                    if now - last_snapshot >= args.snapshot_interval_sec:
                        model.save_snapshot(active=True, stream={"connected": True, "failures": failures})
                        last_snapshot = now
                    if now - model.last_state_save >= args.state_interval_sec:
                        model.save_state()
                    if now - last_tick >= 10.0:
                        log_line(
                            log_path,
                            "micro_lab_tick",
                            received_updates=model.received_updates,
                            changed_updates=model.changed_updates,
                            summaries=model.summaries(),
                        )
                        last_tick = now
                response.close()
                response = None
            except requests.RequestException as exc:
                failures += 1
                log_line(log_path, "micro_stream_error", error_type=type(exc).__name__, error=str(exc)[:300])
                if response is not None:
                    response.close()
                    response = None
                time.sleep(min(15.0, 0.5 * 2 ** min(failures, 5)))
            except Exception as exc:
                failures += 1
                log_line(log_path, "micro_cycle_error", error_type=type(exc).__name__, error=str(exc)[:500])
                if response is not None:
                    response.close()
                    response = None
                time.sleep(min(15.0, 0.5 * 2 ** min(failures, 5)))
    finally:
        if response is not None:
            response.close()
        model.close()
    log_line(
        log_path,
        "micro_lab_end",
        received_updates=model.received_updates,
        changed_updates=model.changed_updates,
        summaries=model.summaries(),
    )
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--creds", type=Path, default=DEFAULT_CREDS)
    parser.add_argument("--account-key", default="OANDA_ACCOUNT_ID_DUM4")
    parser.add_argument("--account-id", default="")
    parser.add_argument("--duration-sec", type=int, default=28800)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument("--prior-count", type=float, default=20.0)
    parser.add_argument("--min-pattern-count", type=int, default=8)
    parser.add_argument("--min-baseline-count", type=int, default=20)
    parser.add_argument("--snapshot-interval-sec", type=float, default=2.0)
    parser.add_argument("--state-interval-sec", type=float, default=60.0)
    parser.add_argument("--retention-hours", type=float, default=72.0)
    parser.add_argument("--prune-interval-sec", type=float, default=3600.0)
    args = parser.parse_args(argv)
    if (
        args.duration_sec <= 0
        or args.prior_count < 0.0
        or args.min_pattern_count <= 0
        or args.min_baseline_count <= 0
        or args.snapshot_interval_sec <= 0.0
        or args.state_interval_sec <= 0.0
        or args.retention_hours < 0.0
        or args.prune_interval_sec <= 0.0
    ):
        raise SystemExit("duration, model counts, and persistence intervals must be positive")
    return args


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
