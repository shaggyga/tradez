#!/usr/bin/env python3
"""Publish continuous, research-only one-hour forecasts for every fresh pair.

The four contributors are deliberately distinct:

* cross-currency impulse from the all-pair relative-strength surface;
* completed-bar multi-timeframe trend;
* a differenced 60-return nearest-analog forecast;
* breakout/change-point hazard from path efficiency and volatility expansion.

Every output is shadow-only. Exact cells must accumulate prospective outcomes
and pass the existing promotion policy before another process can execute them.
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
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    from oanda_model_gap_live_signal_worker import (
        LiveForecastLedger,
        WorkerProcessLock,
    )
    from oanda_signal_contribution_feed import SignalContributionFeed
except ModuleNotFoundError:
    from trad.oanda_model_gap_live_signal_worker import (
        LiveForecastLedger,
        WorkerProcessLock,
    )
    from trad.oanda_signal_contribution_feed import SignalContributionFeed


ROOT = Path(__file__).resolve().parent
STATE_ROOT = ROOT / "data" / "oanda_training_manager" / "state"
DEFAULT_FEATURE_SNAPSHOT = STATE_ROOT / "live_model_feature_snapshot_v1.json"
DEFAULT_MARKET_QUOTES = STATE_ROOT / "practice_007_market_quotes_v1.json"
DEFAULT_SIGNAL_FEED = STATE_ROOT / "practice_007_signal_feed_v1.sqlite"
DEFAULT_LEDGER = STATE_ROOT / "one_hour_shadow_forecasts_v1.sqlite"
DEFAULT_STATE = STATE_ROOT / "one_hour_shadow_signal_v1.json"
DEFAULT_LOCK = STATE_ROOT / "one_hour_shadow_signal_v1.lock"
HORIZON_SEC = 3600
SOURCE = "one_hour_shadow_v1"
FAMILY_AUDIT_WINDOW_DAYS = 7
FAMILY_AUDIT_MAX_SPREAD_PIPS = 3.0
FAMILY_AUDIT_EPISODE_SEC = 900.0
FAMILY_AUDIT_MIN_HOLDOUT_EPISODES = 30

CONTRIBUTORS = (
    {
        "contributor_id": "one_hour.cross_currency_impulse",
        "family": "cross_currency_impulse",
        "hypothesis": (
            "Broad base-versus-quote currency movement leads the pair over the "
            "next hour."
        ),
    },
    {
        "contributor_id": "one_hour.multi_timeframe_trend",
        "family": "multi_timeframe_trend",
        "hypothesis": (
            "Agreement among completed M1/M5/M15/M30/H1 paths carries more "
            "information than a sparse hard-threshold indicator."
        ),
    },
    {
        "contributor_id": "one_hour.analog_60_return_path",
        "family": "differenced_path_analog",
        "hypothesis": (
            "The next 60-minute path resembles historical outcomes following "
            "similar normalized 60-return shapes."
        ),
    },
    {
        "contributor_id": "one_hour.breakout_change_hazard",
        "family": "breakout_change_point",
        "hypothesis": (
            "Efficient path movement, volatility expansion, range location, "
            "and cross-pair breadth identify emerging one-hour moves."
        ),
    },
)


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def finite(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def read_json_if_changed(
    path: Path,
    cached: dict[str, Any],
    cached_mtime_ns: int | None,
) -> tuple[dict[str, Any], int | None, bool]:
    try:
        current_mtime_ns = path.stat().st_mtime_ns
    except OSError:
        return cached, cached_mtime_ns, False
    if current_mtime_ns == cached_mtime_ns:
        return cached, cached_mtime_ns, False
    payload = read_json(path)
    if not payload:
        return cached, cached_mtime_ns, False
    return payload, current_mtime_ns, True


def iso_epoch(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def build_market_quote_snapshot(
    payload: dict[str, Any],
    *,
    now: float,
    max_quote_age_sec: float,
) -> dict[str, Any]:
    payload_epoch = iso_epoch(payload.get("generated_utc"))
    payload_age = (
        max(0.0, now - payload_epoch) if payload_epoch > 0.0 else None
    )
    if (
        payload_epoch > now + 5.0
        or (payload_age is not None and payload_age > max_quote_age_sec)
    ):
        return {}
    raw_quote_epochs = [
        iso_epoch(raw_quote.get("time"))
        for raw_quote in (payload.get("quotes") or {}).values()
        if isinstance(raw_quote, dict) and iso_epoch(raw_quote.get("time")) > 0.0
    ]
    transport_clock_offset_sec = 0.0
    if payload_epoch > 0.0 and raw_quote_epochs:
        newest_quote_epoch = max(raw_quote_epochs)
        recent_epochs = [
            epoch
            for epoch in raw_quote_epochs
            if newest_quote_epoch - epoch <= 60.0
        ]
        candidate_offset = statistics.median(recent_epochs) - payload_epoch
        # Broker clocks can differ from the local receipt clock. Correct only
        # a clear, bounded common offset; small differences remain ordinary
        # quote age and continue to be tested directly.
        if 5.0 < abs(candidate_offset) <= 300.0:
            transport_clock_offset_sec = candidate_offset
    fresh_quotes: dict[str, dict[str, Any]] = {}
    quote_epochs: list[float] = []
    for instrument, raw_quote in (payload.get("quotes") or {}).items():
        quote = raw_quote if isinstance(raw_quote, dict) else {}
        bid = finite(quote.get("bid"))
        ask = finite(quote.get("ask"))
        raw_quote_epoch = iso_epoch(quote.get("time"))
        quote_epoch = raw_quote_epoch - transport_clock_offset_sec
        age = max(0.0, now - quote_epoch) if quote_epoch > 0.0 else math.inf
        if (
            bid <= 0.0
            or ask < bid
            or quote_epoch <= 0.0
            or age > max_quote_age_sec
            or quote_epoch > now + 5.0
        ):
            continue
        fresh_quotes[str(instrument)] = {
            "quote": {
                "bid": bid,
                "ask": ask,
            }
        }
        quote_epochs.append(quote_epoch)
    if not fresh_quotes:
        return {}
    # The minimum timestamp guarantees every included quote was available by
    # the shared observation epoch. Prices can be at most max_quote_age_sec
    # later than that conservative timestamp.
    observed_epoch = min(quote_epochs)
    return {
        "generated_epoch": observed_epoch,
        "generated_utc": datetime.fromtimestamp(
            observed_epoch,
            tz=timezone.utc,
        ).isoformat(),
        "instrument_count": len(fresh_quotes),
        "instruments": fresh_quotes,
        "source": str(payload.get("producer") or "practice_007_market_quotes"),
        "transport_clock_offset_sec": round(transport_clock_offset_sec, 6),
    }


def build_current_market_quote_snapshot(
    payload: dict[str, Any],
    *,
    max_quote_age_sec: float,
    clock: Any = time.time,
) -> dict[str, Any]:
    """Evaluate quote freshness at read time, not at the cycle start.

    A shared-feed write or ledger maintenance can take long enough that live
    quote timestamps legitimately advance beyond the cycle's original clock.
    Reusing that old clock incorrectly labels every current quote as future.
    """

    return build_market_quote_snapshot(
        payload,
        now=float(clock()),
        max_quote_age_sec=max_quote_age_sec,
    )


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


def pip_size(instrument: str, raw: dict[str, Any]) -> float:
    feature_sets = [
        raw.get("features") or {},
        (raw.get("timeframe_features") or {}).get("M1") or {},
    ]
    for features in feature_sets:
        value = finite(features.get("pip"))
        if value > 0.0:
            return value
    return 0.01 if instrument.endswith("_JPY") else 0.0001


def clean_series(values: Any) -> list[float]:
    if not isinstance(values, list):
        return []
    output = []
    for value in values:
        number = finite(value, math.nan)
        if math.isfinite(number) and number > 0.0:
            output.append(number)
    return output


def pip_returns(values: Iterable[float], pip: float) -> list[float]:
    sequence = list(values)
    return [
        (sequence[index] - sequence[index - 1]) / pip
        for index in range(1, len(sequence))
    ]


def root_mean_square(values: list[float]) -> float:
    if not values:
        return 0.0
    return math.sqrt(sum(value * value for value in values) / len(values))


def robust_scale(values: list[float]) -> float:
    if not values:
        return 1.0
    median = statistics.median(values)
    deviations = [abs(value - median) for value in values]
    mad = statistics.median(deviations)
    rms = root_mean_square(values)
    return max(1e-6, 1.4826 * mad, 0.35 * rms)


def base_hour_magnitude(raw: dict[str, Any], pip: float) -> float:
    features = raw.get("features") or {}
    m1 = clean_series((raw.get("series") or {}).get("M1"))
    returns = pip_returns(m1, pip)[-120:]
    one_minute_sigma = root_mean_square(returns)
    atr = max(0.0, finite(features.get("m1_atr14_pips")))
    spread = max(
        0.0,
        finite(
            features.get("live_spread_pips"),
            finite(features.get("current_candle_spread_pips")),
        ),
    )
    diffusion = one_minute_sigma * math.sqrt(60.0) * 0.80
    atr_projection = atr * math.sqrt(60.0) * 0.55
    return max(spread * 1.10, diffusion, atr_projection, 0.10)


def probability_from_score(
    score: float,
    *,
    confidence_scale: float = 1.0,
    maximum_edge: float = 0.22,
) -> float:
    edge = maximum_edge * clamp(confidence_scale, 0.0, 1.0) * math.tanh(score)
    return clamp(0.5 + edge, 0.02, 0.98)


def forecast_candidate(
    *,
    snapshot: dict[str, Any],
    instrument: str,
    raw: dict[str, Any],
    contributor: dict[str, str],
    probability_up: float,
    signed_pips: float,
    magnitude_pips: float,
    diagnostics: dict[str, Any],
) -> dict[str, Any]:
    quote = raw.get("quote") or {}
    pip = pip_size(instrument, raw)
    snapshot_id = str(snapshot.get("snapshot_id") or snapshot.get("generated_epoch"))
    identity = (
        f"{contributor['contributor_id']}|{instrument}|{snapshot_id}|{HORIZON_SEC}"
    )
    candidate_id = (
        "one-hour-"
        + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]
    )
    return {
        "id": candidate_id,
        "model_id": contributor["contributor_id"],
        "family": contributor["family"],
        "profile": "continuous_shadow_v1",
        "signal_role": "structural",
        "instrument": instrument,
        "input_timeframe": "MULTI",
        "generated_epoch": finite(snapshot.get("generated_epoch"), time.time()),
        "generated_utc": str(snapshot.get("generated_utc") or utc_iso()),
        "bid": finite(quote.get("bid")),
        "ask": finite(quote.get("ask")),
        "pip": pip,
        "account_eligible": False,
        "forecast_curve": {
            str(HORIZON_SEC): {
                "probability_up": round(clamp(probability_up, 0.001, 0.999), 8),
                "predicted_signed_pips": round(signed_pips, 8),
                "predicted_magnitude_pips": round(
                    max(abs(signed_pips), magnitude_pips),
                    8,
                ),
                "account_eligible": False,
                "research_only": True,
            }
        },
        "producer_metadata": {
            "schema_version": 1,
            "source": SOURCE,
            "horizon_sec": HORIZON_SEC,
            "hypothesis": contributor["hypothesis"],
            "causal": True,
            "completed_bars_only": True,
            "out_of_fold_required_for_promotion": True,
            "practice_account_execution_authorized": False,
            "real_account_execution_authorized": False,
            "snapshot_id": snapshot_id,
            "feature_origin_utc": raw.get("feature_origin_utc"),
            "diagnostics": diagnostics,
        },
    }


def cross_currency_forecast(
    snapshot: dict[str, Any],
    instrument: str,
    raw: dict[str, Any],
    contributor: dict[str, str],
) -> dict[str, Any] | None:
    features = raw.get("features") or {}
    quote = raw.get("quote") or {}
    if finite(quote.get("bid")) <= 0.0 or finite(quote.get("ask")) <= 0.0:
        return None
    breadth = 2.0 * (finite(features.get("cross_breadth_r3"), 0.5) - 0.5)
    rank = 2.0 * (finite(features.get("pair_rank_r3"), 0.5) - 0.5)
    components = {
        "cross_strength_r1": clamp(
            finite(features.get("cross_strength_r1")), -4.0, 4.0
        ),
        "cross_strength_r3": clamp(
            finite(features.get("cross_strength_r3")), -4.0, 4.0
        ),
        "relative_residual_r3": clamp(
            finite(features.get("relative_residual_r3")), -4.0, 4.0
        ),
        "breadth_centered": clamp(breadth, -1.0, 1.0),
        "rank_centered": clamp(rank, -1.0, 1.0),
    }
    score = (
        0.22 * components["cross_strength_r1"]
        + 0.28 * components["cross_strength_r3"]
        + 0.20 * components["relative_residual_r3"]
        + 0.15 * components["breadth_centered"]
        + 0.15 * components["rank_centered"]
    )
    coverage = clamp(finite(features.get("cross_sample_count")) / 8.0, 0.0, 1.0)
    magnitude = base_hour_magnitude(raw, pip_size(instrument, raw))
    signed = math.tanh(score) * magnitude * 0.70
    return forecast_candidate(
        snapshot=snapshot,
        instrument=instrument,
        raw=raw,
        contributor=contributor,
        probability_up=probability_from_score(score, confidence_scale=coverage),
        signed_pips=signed,
        magnitude_pips=magnitude,
        diagnostics={
            "score": round(score, 8),
            "coverage": round(coverage, 8),
            **{key: round(value, 8) for key, value in components.items()},
        },
    )


def normalized_last_return(values: list[float], pip: float) -> float | None:
    returns = pip_returns(values, pip)
    if len(returns) < 20:
        return None
    scale = robust_scale(returns[-120:])
    return clamp(returns[-1] / scale, -4.0, 4.0)


def multi_timeframe_forecast(
    snapshot: dict[str, Any],
    instrument: str,
    raw: dict[str, Any],
    contributor: dict[str, str],
) -> dict[str, Any] | None:
    quote = raw.get("quote") or {}
    if finite(quote.get("bid")) <= 0.0 or finite(quote.get("ask")) <= 0.0:
        return None
    pip = pip_size(instrument, raw)
    series = raw.get("series") or {}
    weights = {"M1": 0.15, "M5": 0.25, "M15": 0.30, "M30": 0.20, "H1": 0.10}
    components: dict[str, float] = {}
    weighted = 0.0
    total_weight = 0.0
    for timeframe, weight in weights.items():
        value = normalized_last_return(
            clean_series(series.get(timeframe)),
            pip,
        )
        if value is None:
            continue
        components[timeframe] = value
        weighted += weight * value
        total_weight += weight
    if total_weight < 0.50:
        return None
    score = weighted / total_weight
    nonzero = [value for value in components.values() if abs(value) > 1e-9]
    if nonzero:
        positive = sum(value > 0.0 for value in nonzero)
        agreement = max(positive, len(nonzero) - positive) / len(nonzero)
    else:
        agreement = 0.5
    magnitude = base_hour_magnitude(raw, pip)
    signed = math.tanh(score) * magnitude * (0.45 + 0.35 * agreement)
    return forecast_candidate(
        snapshot=snapshot,
        instrument=instrument,
        raw=raw,
        contributor=contributor,
        probability_up=probability_from_score(
            score,
            confidence_scale=agreement,
            maximum_edge=0.20,
        ),
        signed_pips=signed,
        magnitude_pips=magnitude,
        diagnostics={
            "score": round(score, 8),
            "timeframe_weight_coverage": round(total_weight, 8),
            "direction_agreement": round(agreement, 8),
            "normalized_completed_bar_returns": {
                key: round(value, 8) for key, value in components.items()
            },
        },
    )


def z_normalize(values: list[float]) -> tuple[list[float], float]:
    mean = statistics.fmean(values)
    centered = [value - mean for value in values]
    scale = root_mean_square(centered)
    if scale <= 1e-9:
        return [0.0 for _ in values], 0.0
    return [value / scale for value in centered], scale


def analog_path_forecast(
    snapshot: dict[str, Any],
    instrument: str,
    raw: dict[str, Any],
    contributor: dict[str, str],
    *,
    window: int = 60,
    horizon: int = 60,
    neighbors: int = 12,
) -> dict[str, Any] | None:
    quote = raw.get("quote") or {}
    if finite(quote.get("bid")) <= 0.0 or finite(quote.get("ask")) <= 0.0:
        return None
    pip = pip_size(instrument, raw)
    returns = pip_returns(
        clean_series((raw.get("series") or {}).get("M1")),
        pip,
    )
    # Historical labels must end before the current 60-return input begins.
    maximum_end = len(returns) - window - horizon
    if maximum_end < window + neighbors:
        return None
    current, current_scale = z_normalize(returns[-window:])
    if current_scale <= 1e-9:
        return None
    matches: list[tuple[float, float, float]] = []
    stride = 1 if maximum_end < 800 else 2
    for end in range(window, maximum_end + 1, stride):
        candidate, candidate_scale = z_normalize(returns[end - window : end])
        if candidate_scale <= 1e-9:
            continue
        shape_mse = sum(
            (left - right) ** 2 for left, right in zip(current, candidate)
        ) / window
        scale_penalty = 0.10 * abs(
            math.log(max(1e-9, candidate_scale / current_scale))
        )
        distance = shape_mse + scale_penalty
        future = returns[end : end + horizon]
        cumulative = sum(future)
        running = 0.0
        maximum_excursion = 0.0
        for value in future:
            running += value
            maximum_excursion = max(maximum_excursion, abs(running))
        matches.append((distance, cumulative, maximum_excursion))
    matches.sort(key=lambda row: row[0])
    chosen = matches[:neighbors]
    if len(chosen) < max(5, neighbors // 2):
        return None
    weights = [1.0 / max(0.05, row[0]) for row in chosen]
    total_weight = sum(weights)
    signed = sum(
        weight * row[1] for weight, row in zip(weights, chosen)
    ) / total_weight
    magnitude = sum(
        weight * abs(row[1]) for weight, row in zip(weights, chosen)
    ) / total_weight
    expected_excursion = sum(
        weight * row[2] for weight, row in zip(weights, chosen)
    ) / total_weight
    up_weight = sum(
        weight for weight, row in zip(weights, chosen) if row[1] > 0.0
    )
    down_weight = sum(
        weight for weight, row in zip(weights, chosen) if row[1] < 0.0
    )
    directional_agreement = (
        max(up_weight, down_weight) / total_weight if total_weight > 0.0 else 0.5
    )
    base = base_hour_magnitude(raw, pip)
    score = signed / max(base, magnitude, 0.10)
    confidence_scale = clamp(
        (directional_agreement - 0.5) * 2.0,
        0.10,
        1.0,
    )
    return forecast_candidate(
        snapshot=snapshot,
        instrument=instrument,
        raw=raw,
        contributor=contributor,
        probability_up=probability_from_score(
            score,
            confidence_scale=confidence_scale,
            maximum_edge=0.25,
        ),
        signed_pips=signed,
        magnitude_pips=max(magnitude, expected_excursion, base * 0.50),
        diagnostics={
            "window_returns": window,
            "forecast_returns": horizon,
            "candidate_count": len(matches),
            "neighbor_count": len(chosen),
            "mean_neighbor_distance": round(
                statistics.fmean(row[0] for row in chosen),
                8,
            ),
            "directional_agreement": round(directional_agreement, 8),
            "expected_mfe_proxy_pips": round(expected_excursion, 8),
            "current_return_scale_pips": round(current_scale, 8),
        },
    )


def breakout_hazard_forecast(
    snapshot: dict[str, Any],
    instrument: str,
    raw: dict[str, Any],
    contributor: dict[str, str],
) -> dict[str, Any] | None:
    quote = raw.get("quote") or {}
    if finite(quote.get("bid")) <= 0.0 or finite(quote.get("ask")) <= 0.0:
        return None
    pip = pip_size(instrument, raw)
    prices = clean_series((raw.get("series") or {}).get("M1"))
    returns = pip_returns(prices, pip)
    if len(returns) < 90:
        return None
    features = raw.get("features") or {}
    long_scale = robust_scale(returns[-60:])
    short_scale = robust_scale(returns[-10:])
    r5 = sum(returns[-5:])
    r15 = sum(returns[-15:])
    r30 = sum(returns[-30:])
    path = sum(abs(value) for value in returns[-15:])
    efficiency = abs(r15) / max(path, 1e-9)
    change_z = r5 / max(long_scale * math.sqrt(5.0), 1e-9)
    expansion = clamp(short_scale / max(long_scale, 1e-9), 0.25, 4.0)
    range_window = prices[-61:]
    low = min(range_window)
    high = max(range_window)
    range_position = (
        2.0 * (prices[-1] - low) / max(high - low, 1e-12) - 1.0
    )
    breadth = 2.0 * (finite(features.get("cross_breadth_r3"), 0.5) - 0.5)
    volume_ratio = clamp(finite(features.get("volume_ratio_12"), 1.0), 0.0, 4.0)
    direction_score = (
        0.35 * math.tanh(change_z / 2.0)
        + 0.25 * math.tanh(r15 / max(long_scale * math.sqrt(15.0), 1e-9))
        + 0.15 * math.tanh(r30 / max(long_scale * math.sqrt(30.0), 1e-9))
        + 0.15 * range_position
        + 0.10 * clamp(breadth, -1.0, 1.0)
    )
    hazard_logit = (
        1.4 * (expansion - 1.0)
        + 1.2 * (efficiency - 0.35)
        + 0.35 * (volume_ratio - 1.0)
        + 0.45 * abs(breadth)
    )
    hazard = 1.0 / (1.0 + math.exp(-clamp(hazard_logit, -20.0, 20.0)))
    magnitude = base_hour_magnitude(raw, pip) * (0.65 + 0.75 * hazard)
    signed = math.tanh(direction_score) * magnitude * (0.45 + 0.35 * hazard)
    return forecast_candidate(
        snapshot=snapshot,
        instrument=instrument,
        raw=raw,
        contributor=contributor,
        probability_up=probability_from_score(
            direction_score,
            confidence_scale=0.35 + 0.65 * hazard,
            maximum_edge=0.20,
        ),
        signed_pips=signed,
        magnitude_pips=magnitude,
        diagnostics={
            "direction_score": round(direction_score, 8),
            "move_hazard": round(hazard, 8),
            "change_z": round(change_z, 8),
            "volatility_expansion": round(expansion, 8),
            "path_efficiency_15": round(efficiency, 8),
            "range_position_60": round(range_position, 8),
            "volume_ratio_12": round(volume_ratio, 8),
            "cross_breadth_centered": round(breadth, 8),
        },
    )


def generate_one_hour_forecasts(
    snapshot: dict[str, Any],
) -> list[dict[str, Any]]:
    instruments = snapshot.get("instruments") or {}
    if not isinstance(instruments, dict):
        return []
    builders = (
        cross_currency_forecast,
        multi_timeframe_forecast,
        analog_path_forecast,
        breakout_hazard_forecast,
    )
    output: list[dict[str, Any]] = []
    for instrument, raw in sorted(instruments.items()):
        if not isinstance(raw, dict):
            continue
        for contributor, builder in zip(CONTRIBUTORS, builders):
            candidate = builder(snapshot, str(instrument), raw, contributor)
            if candidate is not None:
                output.append(candidate)
    return output


def register_contributors(feed: SignalContributionFeed) -> int:
    return feed.register_contributors(
        [
            {
                **contributor,
                "source_kind": "one_hour_shadow",
                "adapter_module": Path(__file__).name,
                "runtime_policy": "prospective_shadow_then_exact_cell_promotion",
                "expected": True,
                "account_eligible": False,
                "metadata": {
                    "horizon_sec": HORIZON_SEC,
                    "continuous_forecast": True,
                    "causal": True,
                    "practice_account_execution_authorized": False,
                    "real_account_execution_authorized": False,
                },
            }
            for contributor in CONTRIBUTORS
        ]
    )


def _shadow_factor_ids(instrument: str, direction: str) -> set[str]:
    parts = str(instrument or "").split("_")
    if len(parts) != 2:
        return set()
    long_pair = str(direction or "").lower() in {"buy", "long"}
    return {
        f"currency:{parts[0]}:{'long' if long_pair else 'short'}",
        f"currency:{parts[1]}:{'short' if long_pair else 'long'}",
    }


def _payoff(values: Iterable[float]) -> dict[str, float | None]:
    rows = [float(value) for value in values]
    gains = [value for value in rows if value > 0.0]
    losses = [value for value in rows if value < 0.0]
    gross_gain = sum(gains)
    gross_loss = -sum(losses)
    return {
        "average_net_pips": round(statistics.fmean(rows), 6) if rows else None,
        "total_net_pips": round(sum(rows), 6),
        "profit_factor": (
            round(gross_gain / gross_loss, 6) if gross_loss > 0.0 else None
        ),
        "largest_absolute_outcome_removed_average_net_pips": (
            round(
                statistics.fmean(
                    value
                    for index, value in enumerate(rows)
                    if index
                    != max(range(len(rows)), key=lambda item: abs(rows[item]))
                ),
                6,
            )
            if len(rows) > 1
            else None
        ),
    }


def summarize_shadow_family_calibration(
    connection: sqlite3.Connection,
    *,
    now_epoch: float | None = None,
    window_days: int = FAMILY_AUDIT_WINDOW_DAYS,
) -> dict[str, Any]:
    """Report non-overlapping, liquid one-hour family evidence.

    The view is deliberately separate from the live contribution feed.  It
    samples at most one observation per family/pair/horizon wall-clock bucket,
    reports fixed-horizon cost-clearance calibration, and collapses 3-v-1
    minority observations by overlapping signed currency factors.  Nothing in
    this payload can authorize, rank, or route an order.
    """

    now = float(now_epoch if now_epoch is not None else time.time())
    cutoff = now - max(1, int(window_days)) * 86400.0
    rows = connection.execute(
        """
        WITH sampled AS (
            SELECT family, instrument, horizon_sec, generated_epoch,
                   direction, direction_correct, executable_net_pips,
                   signed_mid_move_pips, entry_spread_pips,
                   predicted_magnitude_pips, snapshot_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY family, instrument, horizon_sec,
                                    CAST(generated_epoch / horizon_sec AS INTEGER)
                       ORDER BY generated_epoch, candidate_id
                   ) AS sample_rank
            FROM outcomes
            WHERE status='matured' AND observed_epoch >= ?
              AND entry_spread_pips > 0.0 AND entry_spread_pips <= ?
        )
        SELECT family, instrument, horizon_sec, generated_epoch,
               direction, direction_correct, executable_net_pips,
               signed_mid_move_pips, entry_spread_pips,
               predicted_magnitude_pips, snapshot_id
        FROM sampled
        WHERE sample_rank=1
        ORDER BY generated_epoch, instrument, family
        """,
        (cutoff, FAMILY_AUDIT_MAX_SPREAD_PIPS),
    ).fetchall()
    columns = (
        "family",
        "instrument",
        "horizon_sec",
        "generated_epoch",
        "direction",
        "direction_correct",
        "executable_net_pips",
        "signed_mid_move_pips",
        "entry_spread_pips",
        "predicted_magnitude_pips",
        "snapshot_id",
    )
    sampled = [dict(zip(columns, row)) for row in rows]

    by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in sampled:
        by_family[str(row["family"])].append(row)

    family_cells: list[dict[str, Any]] = []
    calibration_cells: list[dict[str, Any]] = []
    for family, items in sorted(by_family.items()):
        net = [finite(row["executable_net_pips"]) for row in items]
        predicted = [finite(row["predicted_magnitude_pips"]) for row in items]
        actual = [abs(finite(row["signed_mid_move_pips"])) for row in items]
        spreads = [max(1e-12, finite(row["entry_spread_pips"])) for row in items]
        payoff = _payoff(net)
        sessions = {
            datetime.fromtimestamp(finite(row["generated_epoch"]), timezone.utc).hour
            for row in items
        }
        family_cells.append(
            {
                "family": family,
                "non_overlapping_observations": len(items),
                "pairs": len({str(row["instrument"]) for row in items}),
                "utc_hours_observed": len(sessions),
                "direction_accuracy": round(
                    statistics.fmean(int(row["direction_correct"]) for row in items),
                    6,
                ),
                "after_cost_win_rate": round(
                    statistics.fmean(value > 0.0 for value in net), 6
                ),
                "median_predicted_magnitude_pips": round(
                    statistics.median(predicted), 6
                ),
                "median_actual_magnitude_pips": round(
                    statistics.median(actual), 6
                ),
                "actual_clear_1_5x_entry_spread_rate": round(
                    statistics.fmean(
                        movement >= 1.5 * spread
                        for movement, spread in zip(actual, spreads)
                    ),
                    6,
                ),
                "actual_clear_2_0x_entry_spread_rate": round(
                    statistics.fmean(
                        movement >= 2.0 * spread
                        for movement, spread in zip(actual, spreads)
                    ),
                    6,
                ),
                **payoff,
            }
        )
        ratio_bands = (
            ("lt_1x", -math.inf, 1.0),
            ("1_to_1_5x", 1.0, 1.5),
            ("1_5_to_2x", 1.5, 2.0),
            ("2_to_3x", 2.0, 3.0),
            ("ge_3x", 3.0, math.inf),
        )
        for label, lower, upper in ratio_bands:
            band = [
                row
                for row in items
                if lower
                <= finite(row["predicted_magnitude_pips"])
                / max(1e-12, finite(row["entry_spread_pips"]))
                < upper
            ]
            if not band:
                continue
            calibration_cells.append(
                {
                    "family": family,
                    "predicted_magnitude_to_spread_band": label,
                    "n": len(band),
                    "actual_clear_1_5x_entry_spread_rate": round(
                        statistics.fmean(
                            abs(finite(row["signed_mid_move_pips"]))
                            >= 1.5 * finite(row["entry_spread_pips"])
                            for row in band
                        ),
                        6,
                    ),
                    "actual_clear_2_0x_entry_spread_rate": round(
                        statistics.fmean(
                            abs(finite(row["signed_mid_move_pips"]))
                            >= 2.0 * finite(row["entry_spread_pips"])
                            for row in band
                        ),
                        6,
                    ),
                    "direction_accuracy": round(
                        statistics.fmean(
                            int(row["direction_correct"]) for row in band
                        ),
                        6,
                    ),
                    "average_after_cost_pips": round(
                        statistics.fmean(
                            finite(row["executable_net_pips"]) for row in band
                        ),
                        6,
                    ),
                }
            )

    concurrent: dict[tuple[str, int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in sampled:
        key = (
            str(row["instrument"]),
            int(row["horizon_sec"]),
            int(finite(row["generated_epoch"]) // int(row["horizon_sec"])),
        )
        concurrent[key].append(row)
    minority: dict[str, list[dict[str, Any]]] = defaultdict(list)
    majority_outcomes: list[float] = []
    for items in concurrent.values():
        if len({str(row["family"]) for row in items}) != len(CONTRIBUTORS):
            continue
        directions: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in items:
            directions[str(row["direction"])].append(row)
        if sorted(len(group) for group in directions.values()) != [1, 3]:
            continue
        unique_row = next(group[0] for group in directions.values() if len(group) == 1)
        majority_group = next(group for group in directions.values() if len(group) == 3)
        unique_row = dict(unique_row)
        unique_row["factor_ids"] = _shadow_factor_ids(
            str(unique_row["instrument"]), str(unique_row["direction"])
        )
        minority[str(unique_row["family"])].append(unique_row)
        majority_outcomes.append(
            statistics.fmean(
                finite(row["executable_net_pips"]) for row in majority_group
            )
        )

    minority_cells: list[dict[str, Any]] = []
    holdout_candidates: list[dict[str, Any]] = []
    for family, items in sorted(minority.items()):
        episodes: list[dict[str, Any]] = []
        for row in sorted(items, key=lambda item: finite(item["generated_epoch"])):
            observed = finite(row["generated_epoch"])
            factors = set(row["factor_ids"])
            repeat = next(
                (
                    episode
                    for episode in reversed(episodes)
                    if observed - finite(episode["generated_epoch"])
                    <= FAMILY_AUDIT_EPISODE_SEC
                    and factors.intersection(episode["factor_ids"])
                ),
                None,
            )
            if repeat is None:
                episodes.append(row)
        net = [finite(row["executable_net_pips"]) for row in episodes]
        payoff = _payoff(net)
        cell = {
            "family": family,
            "raw_3v1_minority_observations": len(items),
            "independent_factor_episodes": len(episodes),
            "factor_repeats_collapsed": len(items) - len(episodes),
            "pairs": len({str(row["instrument"]) for row in episodes}),
            "direction_accuracy": round(
                statistics.fmean(int(row["direction_correct"]) for row in episodes),
                6,
            ),
            "after_cost_win_rate": round(
                statistics.fmean(value > 0.0 for value in net), 6
            ),
            "best_catches": [
                {
                    "instrument": str(row["instrument"]),
                    "direction": str(row["direction"]),
                    "generated_utc": datetime.fromtimestamp(
                        finite(row["generated_epoch"]), timezone.utc
                    ).isoformat(),
                    "after_cost_pips": round(
                        finite(row["executable_net_pips"]), 6
                    ),
                }
                for row in sorted(
                    episodes,
                    key=lambda item: finite(item["executable_net_pips"]),
                    reverse=True,
                )[:3]
                if finite(row["executable_net_pips"]) > 0.0
            ],
            "worst_misses": [
                {
                    "instrument": str(row["instrument"]),
                    "direction": str(row["direction"]),
                    "generated_utc": datetime.fromtimestamp(
                        finite(row["generated_epoch"]), timezone.utc
                    ).isoformat(),
                    "after_cost_pips": round(
                        finite(row["executable_net_pips"]), 6
                    ),
                }
                for row in sorted(
                    episodes,
                    key=lambda item: finite(item["executable_net_pips"]),
                )[:3]
                if finite(row["executable_net_pips"]) < 0.0
            ],
            **payoff,
        }
        minority_cells.append(cell)
        if (
            len(episodes) >= FAMILY_AUDIT_MIN_HOLDOUT_EPISODES
            and cell["after_cost_win_rate"] >= 0.55
            and finite(cell["average_net_pips"]) > 0.0
            and finite(
                cell["largest_absolute_outcome_removed_average_net_pips"]
            )
            > 0.0
            and finite(cell["profit_factor"]) >= 1.10
            and cell["pairs"] >= 10
        ):
            holdout_candidates.append(
                {
                    **cell,
                    "next_stage": "freeze_then_purged_walk_forward_holdout",
                    "promotion_eligible": False,
                    "execution_eligible": False,
                }
            )

    return {
        "status": "shadow_only_no_execution_hook",
        "generated_utc": datetime.fromtimestamp(now, timezone.utc).isoformat(),
        "window_days": max(1, int(window_days)),
        "sampling_policy": (
            "first matured row per family/pair/horizon wall-clock horizon bucket; "
            "entry spread <=3 pips"
        ),
        "cost_clearance_contract": (
            "fixed-horizon absolute mid move versus 1.5x and 2.0x causal entry spread"
        ),
        "family_cells": family_cells,
        "magnitude_to_cost_calibration": calibration_cells,
        "unique_minority_policy": (
            "3-v-1 family disagreement; minority rows collapsed within 15 minutes "
            "when signed currency factors overlap"
        ),
        "unique_minority_cells": minority_cells,
        "majority_3v1": {
            "observations": len(majority_outcomes),
            **_payoff(majority_outcomes),
        },
        "minimum_independent_episodes_before_holdout": (
            FAMILY_AUDIT_MIN_HOLDOUT_EPISODES
        ),
        "holdout_candidates": holdout_candidates,
        "promotion_eligible": False,
        "execution_eligible": False,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--feature-snapshot", type=Path, default=DEFAULT_FEATURE_SNAPSHOT)
    parser.add_argument("--market-quotes", type=Path, default=DEFAULT_MARKET_QUOTES)
    parser.add_argument("--signal-feed", type=Path, default=DEFAULT_SIGNAL_FEED)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--process-lock", type=Path, default=DEFAULT_LOCK)
    parser.add_argument("--interval-sec", type=float, default=2.0)
    parser.add_argument("--state-interval-sec", type=float, default=30.0)
    parser.add_argument("--family-audit-interval-sec", type=float, default=900.0)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    parser.add_argument("--ttl-sec", type=float, default=600.0)
    parser.add_argument("--max-snapshot-age-sec", type=float, default=600.0)
    parser.add_argument("--max-quote-age-sec", type=float, default=30.0)
    parser.add_argument("--max-outcome-delay-sec", type=float, default=600.0)
    parser.add_argument("--maturity-batch-size", type=int, default=5000)
    parser.add_argument("--retention-days", type=int, default=30)
    parser.add_argument(
        "--mature-only",
        action="store_true",
        help=(
            "Stop producing and publishing forecasts while continuing to mature "
            "the immutable predictions already present in the ledger."
        ),
    )
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args(argv)
    if min(
        args.interval_sec,
        args.state_interval_sec,
        args.family_audit_interval_sec,
        args.duration_sec,
        args.ttl_sec,
        args.max_snapshot_age_sec,
        args.max_quote_age_sec,
        args.max_outcome_delay_sec,
    ) <= 0.0:
        raise SystemExit("worker timing arguments must be positive")
    if args.maturity_batch_size <= 0 or args.retention_days <= 0:
        raise SystemExit("worker retention arguments must be positive")
    return args


def run(args: argparse.Namespace) -> int:
    lock = WorkerProcessLock(args.process_lock)
    if not lock.acquire():
        raise RuntimeError(f"worker already running: {args.process_lock}")
    feed: SignalContributionFeed | None = None
    ledger: LiveForecastLedger | None = None
    try:
        previous = read_json(args.state)
        if args.mature_only:
            registered_contributors = int(
                previous.get("registered_contributors") or 0
            )
        else:
            feed = SignalContributionFeed(args.signal_feed)
            registered_contributors = register_contributors(feed)
        ledger = LiveForecastLedger(args.ledger, args.retention_days)
        last_snapshot_id = str(previous.get("feature_snapshot_id") or "")
        cycles = int(previous.get("cycles") or 0)
        published = int(previous.get("published_forecasts") or 0)
        rejected = int(previous.get("rejected_forecasts") or 0)
        prediction_points = int(previous.get("registered_prediction_points") or 0)
        matured_points = int(previous.get("matured_prediction_points") or 0)
        censored_points = int(previous.get("censored_prediction_points") or 0)
        errors = int(previous.get("errors") or 0)
        session_started_utc = utc_iso()
        session_cycles = 0
        session_errors = 0
        session_published = 0
        session_rejected = 0
        session_matured = 0
        session_censored = 0
        last_error = previous.get("last_error")
        last_success_utc = str(previous.get("last_success_utc") or "")
        ledger_summary = previous.get("ledger") or {}
        family_audit = previous.get("family_audit") or {}
        feed_coverage = previous.get("feed") or {}
        last_maintenance_at = -math.inf
        last_family_audit_at = -math.inf
        cached_snapshot: dict[str, Any] = {}
        snapshot_mtime_ns: int | None = None
        cached_market_quotes: dict[str, Any] = {}
        market_quotes_mtime_ns: int | None = None
        outcome_quote_source = "feature_snapshot"
        outcome_quote_count = 0
        outcome_quote_age_sec: float | None = None
        stop_at = time.monotonic() + args.duration_sec
        while time.monotonic() < stop_at:
            started = time.monotonic()
            cycles += 1
            session_cycles += 1
            maintenance_due = (
                args.once
                or started - last_maintenance_at >= args.state_interval_sec
            )
            published_snapshot = False
            now = time.time()
            snapshot, snapshot_mtime_ns, _ = read_json_if_changed(
                args.feature_snapshot,
                cached_snapshot,
                snapshot_mtime_ns,
            )
            cached_snapshot = snapshot
            generated = finite(snapshot.get("generated_epoch"))
            snapshot_id = str(snapshot.get("snapshot_id") or generated or "")
            age = math.inf if generated <= 0.0 else max(0.0, now - generated)
            status = (
                "draining_pending_outcomes"
                if args.mature_only
                else "waiting_for_feature_snapshot"
            )
            publish_result: dict[str, Any] = {}
            generator_counts: dict[str, int] = {}
            if args.mature_only:
                # Retirement is prospective only: keep the historical forecast
                # contract immutable and let every already-issued horizon mature,
                # but do not touch the shared contribution feed or add predictions.
                pass
            elif snapshot and age <= args.max_snapshot_age_sec:
                try:
                    if snapshot_id and snapshot_id != last_snapshot_id:
                        assert feed is not None
                        raw_forecasts = generate_one_hour_forecasts(snapshot)
                        for row in raw_forecasts:
                            model_id = str(row.get("model_id") or "")
                            generator_counts[model_id] = (
                                generator_counts.get(model_id, 0) + 1
                            )
                        normalized = [
                            feed.normalize_forecast(
                                row,
                                SOURCE,
                                now=now,
                                max_age_sec=args.max_snapshot_age_sec,
                            )
                            for row in raw_forecasts
                        ]
                        publish_result = feed.publish_forecasts(
                            raw_forecasts,
                            SOURCE,
                            args.ttl_sec,
                            max_age_sec=args.max_snapshot_age_sec,
                        )
                        publish_result.pop("candidate_ids", None)
                        prediction_points += ledger.register(
                            normalized,
                            snapshot_id,
                            minimum_target_epoch=now - args.max_outcome_delay_sec,
                        )
                        published += int(publish_result.get("accepted") or 0)
                        rejected += int(publish_result.get("rejected") or 0)
                        session_published += int(
                            publish_result.get("accepted") or 0
                        )
                        session_rejected += int(
                            publish_result.get("rejected") or 0
                        )
                        last_snapshot_id = snapshot_id
                        status = "published"
                        published_snapshot = True
                        last_success_utc = utc_iso()
                    else:
                        status = "snapshot_already_processed"
                except Exception as exc:
                    # A failed shared-feed write can leave this long-lived
                    # connection holding a transaction/read snapshot. Roll it
                    # back immediately so WAL checkpoints are not pinned by a
                    # shadow-only worker.
                    try:
                        feed.connection.rollback()
                    except sqlite3.Error:
                        pass
                    errors += 1
                    session_errors += 1
                    last_error = {
                        "time": utc_iso(),
                        "stage": "publish",
                        "kind": type(exc).__name__,
                        "message": str(exc)[:500],
                    }
                    status = f"error:{type(exc).__name__}:{exc}"
            elif snapshot:
                status = "stale_feature_snapshot"
            if published_snapshot or maintenance_due:
                try:
                    (
                        cached_market_quotes,
                        market_quotes_mtime_ns,
                        _,
                    ) = read_json_if_changed(
                        args.market_quotes,
                        cached_market_quotes,
                        market_quotes_mtime_ns,
                    )
                    quote_snapshot = build_current_market_quote_snapshot(
                        cached_market_quotes,
                        max_quote_age_sec=args.max_quote_age_sec,
                    )
                    outcome_snapshot = quote_snapshot or snapshot
                    outcome_quote_source = (
                        "practice_007_market_quotes"
                        if quote_snapshot
                        else "feature_snapshot_fallback"
                    )
                    outcome_quote_count = int(
                        finite(outcome_snapshot.get("instrument_count"))
                    )
                    outcome_quote_age_sec = max(
                        0.0,
                        time.time()
                        - finite(
                            outcome_snapshot.get("generated_epoch"),
                            time.time(),
                        ),
                    )
                    outcome = ledger.mature(
                        outcome_snapshot,
                        args.max_outcome_delay_sec,
                        batch_size=args.maturity_batch_size,
                        censor_missing_quotes=False,
                    )
                    matured_points += int(outcome.get("matured") or 0)
                    censored_points += int(outcome.get("censored") or 0)
                    session_matured += int(outcome.get("matured") or 0)
                    session_censored += int(outcome.get("censored") or 0)
                    last_success_utc = utc_iso()
                except Exception as exc:
                    errors += 1
                    session_errors += 1
                    last_error = {
                        "time": utc_iso(),
                        "stage": "outcome_maintenance",
                        "kind": type(exc).__name__,
                        "message": str(exc)[:500],
                    }
                    status = f"error:{type(exc).__name__}:{exc}"
            if published_snapshot or maintenance_due or status.startswith("error:"):
                ledger_summary = ledger.summary()
                if (
                    args.once
                    or started - last_family_audit_at
                    >= args.family_audit_interval_sec
                ):
                    family_audit = summarize_shadow_family_calibration(
                        ledger.connection,
                        now_epoch=time.time(),
                    )
                    last_family_audit_at = started
                if feed is not None:
                    feed_coverage = feed.coverage(fresh_sec=args.ttl_sec)
                if args.mature_only:
                    status = (
                        "drained_retired"
                        if int(ledger_summary.get("pending") or 0) == 0
                        else "draining_pending_outcomes"
                    )
                state = {
                    "schema_version": 3,
                    "updated_utc": utc_iso(),
                    "status": status,
                    "cycles": cycles,
                    "errors": errors,
                    "last_error": last_error,
                    "last_success_utc": last_success_utc,
                    "session": {
                        "started_utc": session_started_utc,
                        "cycles": session_cycles,
                        "errors": session_errors,
                        "published_forecasts": session_published,
                        "rejected_forecasts": session_rejected,
                        "matured_prediction_points": session_matured,
                        "censored_prediction_points": session_censored,
                    },
                    "feature_snapshot": str(args.feature_snapshot.resolve()),
                    "market_quotes": str(args.market_quotes.resolve()),
                    "feature_snapshot_id": last_snapshot_id,
                    "feature_snapshot_age_sec": (
                        round(age, 6) if math.isfinite(age) else None
                    ),
                    "snapshot_instruments": int(
                        finite(snapshot.get("instrument_count"))
                    ),
                    "registered_contributors": registered_contributors,
                    "generator_counts": generator_counts,
                    "publish": publish_result,
                    "published_forecasts": published,
                    "rejected_forecasts": rejected,
                    "registered_prediction_points": prediction_points,
                    "matured_prediction_points": matured_points,
                    "censored_prediction_points": censored_points,
                    "outcome_quote_source": outcome_quote_source,
                    "outcome_quote_count": outcome_quote_count,
                    "outcome_quote_age_sec": outcome_quote_age_sec,
                    "ledger": ledger_summary,
                    "family_audit": family_audit,
                    "feed": feed_coverage,
                    "runtime_mode": (
                        "mature_only" if args.mature_only else "produce_and_mature"
                    ),
                    "contract": {
                        "account_scope": "practice_007_shadow_observation",
                        "account_execution_authorized": False,
                        "practice_account_execution_authorized": False,
                        "real_account_execution_authorized": False,
                        "continuous_one_hour_coverage": not args.mature_only,
                        "future_forecast_production": not args.mature_only,
                        "pending_forecasts_preserved_to_maturity": True,
                        "completed_bars_only": True,
                        "prospective_validation_required": True,
                        "family_audit_can_change_execution": False,
                    },
                }
                atomic_json(args.state, state)
                last_maintenance_at = time.monotonic()
            if args.once:
                break
            elapsed = time.monotonic() - started
            sleep_interval = args.interval_sec
            if (
                args.mature_only
                and int(ledger_summary.get("pending") or 0) == 0
            ):
                # A fully drained retirement worker only needs to leave a fresh
                # liveness marker. Avoid continuously rescanning the historical
                # ledger once no causal outcomes remain to mature.
                sleep_interval = max(sleep_interval, 240.0)
            time.sleep(max(0.05, sleep_interval - elapsed))
        return 0
    finally:
        if ledger is not None:
            ledger.close()
        if feed is not None:
            feed.close()
        lock.release()


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
