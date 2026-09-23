#!/usr/bin/env python3
"""Continuous shadow equations across input timeframes and outcome horizons."""

from __future__ import annotations

import math
import json
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


TIMEFRAME_SECONDS = {
    "S5": 5,
    "S10": 10,
    "S15": 15,
    "S30": 30,
    "M1": 60,
    "M5": 300,
    "M10": 600,
    "M15": 900,
    "M30": 1800,
    "H1": 3600,
    "H2": 7200,
    "H3": 10800,
    "H4": 14400,
}

FEATURE_SERIES = {
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


def finite(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


@dataclass(frozen=True)
class QuotePoint:
    epoch: int
    mid: float


class TimeframeHorizonMatrix:
    """Track one-second quotes and emit research-only continuous forecasts."""

    def __init__(
        self,
        max_quote_seconds: int = 1800,
        emit_interval_sec: int = 300,
        calibration_state_path: Path | None = None,
        calibration_refresh_sec: float = 60.0,
        minimum_timeframe_sec: int = 5,
        outcome_horizon_mode: str = "full",
    ) -> None:
        self.max_quote_seconds = max(900, int(max_quote_seconds))
        self.emit_interval_sec = max(60, int(emit_interval_sec))
        self.points: dict[str, deque[QuotePoint]] = defaultdict(
            lambda: deque(maxlen=self.max_quote_seconds)
        )
        self.last_observed_epoch = -1
        self.last_emit_epoch = -10**9
        self.last_origin: dict[tuple[str, str], str] = {}
        self.cycles = 0
        self.forecasts = 0
        self.ready_timeframes: set[str] = set()
        self.calibration_state_path = (
            None if calibration_state_path is None else Path(calibration_state_path)
        )
        self.calibration_refresh_sec = max(5.0, float(calibration_refresh_sec))
        self.calibration_loaded_monotonic = -10**9
        self.calibration_mtime_ns = -1
        self.calibration_state: dict[str, Any] = {}
        self.minimum_timeframe_sec = max(1, int(minimum_timeframe_sec))
        if outcome_horizon_mode not in {"full", "native"}:
            raise ValueError("outcome_horizon_mode must be full or native")
        self.outcome_horizon_mode = outcome_horizon_mode
        self.active_timeframes = {
            label: seconds
            for label, seconds in TIMEFRAME_SECONDS.items()
            if seconds >= self.minimum_timeframe_sec
        }

    def _load_calibration(self) -> None:
        if self.calibration_state_path is None:
            return
        now = time.monotonic()
        if now - self.calibration_loaded_monotonic < self.calibration_refresh_sec:
            return
        self.calibration_loaded_monotonic = now
        try:
            stat = self.calibration_state_path.stat()
            if stat.st_mtime_ns == self.calibration_mtime_ns:
                return
            payload = json.loads(
                self.calibration_state_path.read_text(encoding="utf-8")
            )
            if isinstance(payload, dict):
                self.calibration_state = payload
                self.calibration_mtime_ns = stat.st_mtime_ns
        except (OSError, json.JSONDecodeError):
            return

    def _apply_calibration(
        self,
        forecast: dict[str, Any],
        *,
        instrument: str,
        timeframe: str,
        horizon_sec: int,
    ) -> dict[str, Any]:
        self._load_calibration()
        raw = max(0.001, min(0.999, finite(forecast.get("probability_up"), 0.5)))
        lane_id = f"timeframe_equation_matrix.{timeframe.lower()}"
        pair_key = f"{instrument}|{lane_id}|{int(horizon_sec)}"
        global_key = f"{lane_id}|{int(horizon_sec)}"
        surface = (self.calibration_state.get("pair_surfaces") or {}).get(pair_key)
        scope = "pair"
        if not isinstance(surface, dict) or int(finite(surface.get("n"))) < 20:
            surface = (self.calibration_state.get("global_surfaces") or {}).get(
                global_key
            )
            scope = "global"
        calibrated = raw
        if isinstance(surface, dict):
            bins = surface.get("bins") or []
            index = min(9, int(raw * 10))
            if index < len(bins) and isinstance(bins[index], dict):
                calibrated = max(
                    0.001,
                    min(0.999, finite(bins[index].get("posterior_up"), raw)),
                )
        forecast["raw_probability_up"] = round(raw, 6)
        forecast["calibrated_probability_up"] = round(calibrated, 6)
        forecast["probability_up"] = round(calibrated, 6)
        forecast["calibration_scope"] = scope if isinstance(surface, dict) else "none"
        forecast["calibration_n"] = (
            int(finite(surface.get("n"))) if isinstance(surface, dict) else 0
        )
        forecast["calibration_ready"] = bool(
            isinstance(surface, dict) and surface.get("validation_ready")
        )
        forecast["calibrated_brier"] = (
            surface.get("calibrated_brier") if isinstance(surface, dict) else None
        )
        forecast["account_eligible"] = False
        return forecast

    def observe(
        self,
        prices: dict[str, Any],
        now_epoch: float | None = None,
    ) -> None:
        epoch = int(time.time() if now_epoch is None else now_epoch)
        if epoch <= self.last_observed_epoch:
            return
        self.last_observed_epoch = epoch
        for instrument, quote in prices.items():
            bid = finite(getattr(quote, "bid", 0.0))
            ask = finite(getattr(quote, "ask", 0.0))
            if bid <= 0.0 or ask <= 0.0:
                continue
            self.points[instrument].append(
                QuotePoint(epoch=epoch, mid=(bid + ask) / 2.0)
            )

    def _subminute_series(
        self,
        instrument: str,
        timeframe_sec: int,
    ) -> tuple[list[float], str]:
        rows = self.points.get(instrument)
        if not rows:
            return [], ""
        buckets: dict[int, float] = {}
        for point in rows:
            buckets[point.epoch // timeframe_sec] = point.mid
        ordered = sorted(buckets.items())
        if not ordered:
            return [], ""
        return [value for _, value in ordered], str(ordered[-1][0])

    @staticmethod
    def _equation(
        values: list[float],
        pip: float,
        timeframe_sec: int,
        horizon_sec: int,
        spread_pips: float,
    ) -> dict[str, Any] | None:
        clean = [finite(value) for value in values if finite(value) > 0.0]
        if len(clean) < 8 or pip <= 0.0:
            return None
        window = clean[-min(32, len(clean)) :]
        count = len(window)
        x_mean = (count - 1) / 2.0
        y_mean = statistics.fmean(window)
        denominator = sum((index - x_mean) ** 2 for index in range(count))
        if denominator <= 0.0:
            return None
        slope = sum(
            (index - x_mean) * (value - y_mean)
            for index, value in enumerate(window)
        ) / denominator
        fitted = [
            y_mean + slope * (index - x_mean)
            for index in range(count)
        ]
        residual_pips = math.sqrt(
            statistics.fmean(
                ((actual - expected) / pip) ** 2
                for actual, expected in zip(window, fitted)
            )
        )
        recent_per_bar = (window[-1] - window[max(0, count - 5)]) / (
            pip * max(1, min(4, count - 1))
        )
        slope_pips_per_bar = slope / pip
        blended_per_bar = 0.70 * slope_pips_per_bar + 0.30 * recent_per_bar
        steps = max(0.05, float(horizon_sec) / max(1.0, float(timeframe_sec)))
        damping = 1.0 / math.sqrt(max(1.0, steps / 8.0))
        predicted_signed_pips = blended_per_bar * steps * damping
        scale = max(
            0.20,
            spread_pips * 1.5,
            residual_pips * math.sqrt(max(1.0, steps)),
        )
        z_score = max(-8.0, min(8.0, predicted_signed_pips / scale))
        probability_up = 1.0 / (1.0 + math.exp(-z_score))
        return {
            "horizon_sec": int(horizon_sec),
            "steps_ahead": round(steps, 4),
            "slope_pips_per_bar": round(slope_pips_per_bar, 6),
            "recent_pips_per_bar": round(recent_per_bar, 6),
            "residual_rmse_pips": round(residual_pips, 6),
            "predicted_signed_pips": round(predicted_signed_pips, 6),
            "probability_up": round(probability_up, 6),
            "movement_coefficient": round(
                abs(predicted_signed_pips) / max(residual_pips, 0.20),
                6,
            ),
        }

    def emit(
        self,
        feature_cache: dict[str, dict[str, Any]],
        prices: dict[str, Any],
        pip_sizes: dict[str, float],
        horizons: list[float],
        pending: list[dict[str, Any]],
        log: Callable[..., None],
        candidate_sink: list[dict[str, Any]] | None = None,
    ) -> int:
        if self.last_observed_epoch - self.last_emit_epoch < self.emit_interval_sec:
            return 0
        emitted = 0
        top: list[dict[str, Any]] = []
        configured_horizons = sorted(
            {int(value) for value in horizons if int(value) > 0}
        )
        for instrument, quote in prices.items():
            pip = finite(pip_sizes.get(instrument))
            bid = finite(getattr(quote, "bid", 0.0))
            ask = finite(getattr(quote, "ask", 0.0))
            if pip <= 0.0 or bid <= 0.0 or ask <= 0.0:
                continue
            spread_pips = (ask - bid) / pip
            features = feature_cache.get(instrument) or {}
            for timeframe, timeframe_sec in self.active_timeframes.items():
                if timeframe in {"S5", "S10", "S15", "S30"}:
                    values, origin = self._subminute_series(
                        instrument,
                        timeframe_sec,
                    )
                else:
                    values = list(features.get(FEATURE_SERIES[timeframe]) or [])
                    origin = str(
                        (features.get("series_origins") or {}).get(timeframe)
                        or features.get("candle_time")
                        or ""
                    )
                if len(values) < 8 or not origin:
                    continue
                key = (instrument, timeframe)
                if self.last_origin.get(key) == origin:
                    continue
                curve_horizons = configured_horizons
                if self.outcome_horizon_mode == "native" and configured_horizons:
                    curve_horizons = [
                        min(
                            configured_horizons,
                            key=lambda value: abs(int(value) - int(timeframe_sec)),
                        )
                    ]
                curve: dict[str, dict[str, Any]] = {}
                for horizon in curve_horizons:
                    forecast = self._equation(
                        values,
                        pip,
                        timeframe_sec,
                        horizon,
                        spread_pips,
                    )
                    if forecast is not None:
                        curve[str(horizon)] = self._apply_calibration(
                            forecast,
                            instrument=instrument,
                            timeframe=timeframe,
                            horizon_sec=horizon,
                        )
                if not curve:
                    continue
                self.last_origin[key] = origin
                self.ready_timeframes.add(timeframe)
                anchor = curve.get("300") or curve[str(min(map(int, curve)))]
                direction = (
                    "buy"
                    if finite(anchor.get("probability_up"), 0.5) >= 0.5
                    else "sell"
                )
                lane_id = f"timeframe_equation_matrix.{timeframe.lower()}"
                event_id = (
                    f"tf-matrix-{instrument}-{timeframe.lower()}-"
                    f"{origin.replace(':', '').replace('-', '')}"
                )
                pending.append(
                    {
                        "id": event_id,
                        "lane_id": lane_id,
                        "family": "timeframe_equation_matrix",
                        "profile": "continuous",
                        "model_id": lane_id,
                        "input_timeframe": timeframe,
                        "training_timeframe": "rolling_live_equation",
                        "kind": "signal",
                        "instrument": instrument,
                        "direction": direction,
                        "blocked_reason": "research_only",
                        "miss_class": "",
                        "entry_bid": bid,
                        "entry_ask": ask,
                        "entry_mid": (bid + ask) / 2.0,
                        "entry_time": str(getattr(quote, "time", "")),
                        "pip": pip,
                        "spread_pips": spread_pips,
                        "volatility_regime": "unknown",
                        "forecast_curve": curve,
                        "stop_loss_pips": 5.0,
                        "take_profit_r": 1.5,
                        "max_favorable_pips": 0.0,
                        "max_adverse_pips": 0.0,
                        "favorable_hits": {},
                        "adverse_hits": {},
                        "path_samples": 0,
                        "track_exit_path": False,
                        "created_monotonic": time.monotonic(),
                        "remaining_horizons": [
                            float(value)
                            for value in sorted(map(int, curve))
                        ],
                    }
                )
                if candidate_sink is not None:
                    candidate_sink.append(
                        {
                            "id": event_id,
                            "lane_id": lane_id,
                            "family": "timeframe_equation_matrix",
                            "profile": "continuous",
                            "model_id": lane_id,
                            "input_timeframe": timeframe,
                            "training_timeframe": timeframe,
                            "signal_role": "structural",
                            "instrument": instrument,
                            "direction": direction,
                            "bid": bid,
                            "ask": ask,
                            "entry_time": str(getattr(quote, "time", "")),
                            "pip": pip,
                            "spread_pips": spread_pips,
                            "signal_to_spread": (
                                abs(finite(anchor.get("predicted_signed_pips")))
                                / max(spread_pips, 0.10)
                            ),
                            "predicted_signed_pips": anchor.get(
                                "predicted_signed_pips"
                            ),
                            "probability_up": anchor.get("probability_up"),
                            "forecast_curve": curve,
                            "account_eligible": False,
                            "research_only": True,
                            "research_blocked_reason": "timeframe_surface_validation",
                        }
                    )
                top.append(
                    {
                        "instrument": instrument,
                        "input_timeframe": timeframe,
                        "direction": direction,
                        "anchor_horizon_sec": int(anchor["horizon_sec"]),
                        "predicted_signed_pips": anchor["predicted_signed_pips"],
                        "probability_up": anchor["probability_up"],
                        "raw_probability_up": anchor["raw_probability_up"],
                        "calibration_scope": anchor["calibration_scope"],
                        "calibration_n": anchor["calibration_n"],
                        "calibration_ready": anchor["calibration_ready"],
                        "movement_coefficient": anchor["movement_coefficient"],
                    }
                )
                emitted += 1
        if emitted:
            self.last_emit_epoch = self.last_observed_epoch
            top.sort(
                key=lambda row: abs(finite(row.get("predicted_signed_pips"))),
                reverse=True,
            )
            log(
                "timeframe_matrix_forecast_batch",
                count=emitted,
                input_timeframes=sorted(self.ready_timeframes),
                horizons_sec=configured_horizons,
                top=top[:20],
                account_eligible=False,
            )
        self.cycles += 1
        self.forecasts += emitted
        return emitted

    def summary(self, horizons: list[float] | tuple[float, ...] = ()) -> dict[str, Any]:
        configured_horizons = sorted(
            {int(value) for value in horizons if int(value) > 0}
        )
        return {
            "model_family": "timeframe_equation_matrix",
            "input_timeframes": list(self.active_timeframes),
            "training_timeframe": "rolling_live_equation",
            "physical_lane_count": len(self.active_timeframes),
            "horizons_sec": configured_horizons,
            "lane_horizon_surfaces": (
                len(self.active_timeframes)
                if self.outcome_horizon_mode == "native" and configured_horizons
                else len(self.active_timeframes) * len(configured_horizons)
            ),
            "outcome_horizon_mode": self.outcome_horizon_mode,
            "ready_timeframes": sorted(self.ready_timeframes),
            "cycles": self.cycles,
            "forecasts": self.forecasts,
            "account_eligible": False,
            "status": (
                "shadow_calibrated"
                if int(finite(self.calibration_state.get("ready_surface_count"))) > 0
                else "shadow_collecting"
            ),
            "calibration": {
                "state": (
                    ""
                    if self.calibration_state_path is None
                    else str(self.calibration_state_path.resolve())
                ),
                "surface_count": int(
                    finite(self.calibration_state.get("surface_count"))
                ),
                "ready_surface_count": int(
                    finite(self.calibration_state.get("ready_surface_count"))
                ),
                "generated_utc": str(
                    self.calibration_state.get("generated_utc") or ""
                ),
                "account_eligible": False,
            },
            "emit_interval_sec": self.emit_interval_sec,
        }


__all__ = ["TIMEFRAME_SECONDS", "TimeframeHorizonMatrix"]
