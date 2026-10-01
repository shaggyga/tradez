#!/usr/bin/env python3
"""Compare FX strategy variants and optionally execute top practice signals.

All lanes always retain independent shadow entries and outcomes. Optional
execution is restricted to the OANDA practice endpoint. Individual candidates
from every feed are ranked together; historical lane evidence calibrates signal
confidence without requiring an entire lane to be promoted.
"""

from __future__ import annotations

import argparse
import atexit
import hashlib
import json
import math
import os
import re
import secrets
import sqlite3
import statistics
import threading
import time
from collections import Counter, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

try:
    from oanda_supervised_m5_contract_v2 import (
        CONTRACT as SUPERVISED_DATA_CONTRACT, input_fingerprint as supervised_input_fingerprint,
        supervised_candle_data as supervised_candle_data_v2,
    )
except ModuleNotFoundError:
    from trad.oanda_supervised_m5_contract_v2 import (
        CONTRACT as SUPERVISED_DATA_CONTRACT, input_fingerprint as supervised_input_fingerprint,
        supervised_candle_data as supervised_candle_data_v2,
    )

import numpy as np
import requests

try:
    from oanda_feature_observations_v1 import (
        DEFAULT_ARCHIVE_ROOT as DEFAULT_OBSERVATION_ARCHIVE_ROOT,
        PRODUCER_CONTRACT as OBSERVATION_PRODUCER_CONTRACT,
        archive_observation_snapshot,
        capture_feature_group,
        normalize_json as normalize_observation_json,
        observation_source_sha256,
    )
except ModuleNotFoundError:
    from trad.oanda_feature_observations_v1 import (
        DEFAULT_ARCHIVE_ROOT as DEFAULT_OBSERVATION_ARCHIVE_ROOT,
        PRODUCER_CONTRACT as OBSERVATION_PRODUCER_CONTRACT,
        archive_observation_snapshot,
        capture_feature_group,
        normalize_json as normalize_observation_json,
        observation_source_sha256,
    )

try:
    from oanda_entry_diagnostics import selection_fields, blocked_fields
    from oanda_intrahour_forecast_contract import (
        contract_payload as intrahour_contract_payload,
        filter_context_views,
        flatten_context_views,
        structural_feature_view,
    )
    from oanda_pattern_count_forecast import (
        DEFAULT_CACHE as DEFAULT_PATTERN_HISTORY_CACHE,
        DEFAULT_LIVE_STATE as DEFAULT_PATTERN_LIVE_STATE,
        PatternCountForecaster,
    )
    from oanda_practice_all_pairs_opportunity_scalper import (
        parse_instrument_list,
    )
    from oanda_practice_eurusd_micro_scalper import (
        BASE_URL,
        DEFAULT_CREDS,
        DEFAULT_LOG_DIR,
        OandaApiError,
        STREAM_URL,
        candle_closes,
        ema_series,
        infer_pip_size,
        macd_histogram,
        parse_rfc3339,
        quote_from_price_payload,
        rsi_series,
        safe_float,
    )
    from oanda_practice_pair_rotation_scalper import (
        read_credentials,
        tradeable_currency_instruments,
    )
    from oanda_signal_combination_audit import (
        SignalCombinationModel,
        SignalCombinationStore,
        build_signal_vector,
    )
    from oanda_sma_signal_filter import (
        SmaSignalFilterModel,
        SmaSignalFilterStore,
        build_sma_signal_vector,
        filter_point_for_horizon,
        validated_filter_weight,
    )
    from oanda_lane_promotion import LanePromotionModel, LanePromotionStore, PromotionThresholds
    from oanda_ma_feature_grid import MaFeatureGridRuntime
    from oanda_execution_policy import FrozenExecutionPolicy
    from oanda_second_forecast import SecondForecastRuntime
    from oanda_signal_contribution_feed import SignalContributionFeed
    from oanda_shadow_outcome_store import ShadowOutcomeStore, sampled_detail
    from oanda_strategy_archetypes import archetype_vote_summary, strategy_archetype
    from oanda_strategy_exit_fit import PATH_LEVELS, StrategyExitFit, level_key
    from oanda_timeframe_horizon_matrix import TimeframeHorizonMatrix
    from oanda_worker_heartbeat import WorkerHeartbeat
    from oanda_quote_transport import QuoteSnapshotPublisher, load_quote_snapshot
except ModuleNotFoundError:  # Package imports used by the test suite.
    from trad.oanda_entry_diagnostics import selection_fields, blocked_fields
    from trad.oanda_intrahour_forecast_contract import (
        contract_payload as intrahour_contract_payload,
        filter_context_views,
        flatten_context_views,
        structural_feature_view,
    )
    from trad.oanda_pattern_count_forecast import (
        DEFAULT_CACHE as DEFAULT_PATTERN_HISTORY_CACHE,
        DEFAULT_LIVE_STATE as DEFAULT_PATTERN_LIVE_STATE,
        PatternCountForecaster,
    )
    from trad.oanda_practice_all_pairs_opportunity_scalper import (
        parse_instrument_list,
    )
    from trad.oanda_practice_eurusd_micro_scalper import (
        BASE_URL,
        DEFAULT_CREDS,
        DEFAULT_LOG_DIR,
        OandaApiError,
        STREAM_URL,
        candle_closes,
        ema_series,
        infer_pip_size,
        macd_histogram,
        parse_rfc3339,
        quote_from_price_payload,
        rsi_series,
        safe_float,
    )
    from trad.oanda_practice_pair_rotation_scalper import (
        read_credentials,
        tradeable_currency_instruments,
    )
    from trad.oanda_signal_combination_audit import (
        SignalCombinationModel,
        SignalCombinationStore,
        build_signal_vector,
    )
    from trad.oanda_sma_signal_filter import (
        SmaSignalFilterModel,
        SmaSignalFilterStore,
        build_sma_signal_vector,
        filter_point_for_horizon,
        validated_filter_weight,
    )
    from trad.oanda_lane_promotion import LanePromotionModel, LanePromotionStore, PromotionThresholds
    from trad.oanda_ma_feature_grid import MaFeatureGridRuntime
    from trad.oanda_execution_policy import FrozenExecutionPolicy
    from trad.oanda_second_forecast import SecondForecastRuntime
    from trad.oanda_signal_contribution_feed import SignalContributionFeed
    from trad.oanda_shadow_outcome_store import ShadowOutcomeStore, sampled_detail
    from trad.oanda_strategy_archetypes import archetype_vote_summary, strategy_archetype
    from trad.oanda_strategy_exit_fit import PATH_LEVELS, StrategyExitFit, level_key
    from trad.oanda_timeframe_horizon_matrix import TimeframeHorizonMatrix
    from trad.oanda_worker_heartbeat import WorkerHeartbeat
    from trad.oanda_quote_transport import QuoteSnapshotPublisher, load_quote_snapshot


NEW_YORK_TZ = ZoneInfo("America/New_York")

MICROSTRUCTURE_SCALAR_KEYS = (
    "bid_top_liquidity",
    "ask_top_liquidity",
    "bid_total_liquidity",
    "ask_total_liquidity",
    "depth_imbalance",
    "bid_levels",
    "ask_levels",
    "microprice",
    "order_book_near_5_imbalance",
    "order_book_near_10_imbalance",
    "order_book_near_25_imbalance",
    "order_book_near_50_imbalance",
    "order_book_above_25_net",
    "order_book_below_25_net",
    "order_book_long_peak_distance_pips",
    "order_book_short_peak_distance_pips",
    "order_book_concentration",
    "position_book_near_5_imbalance",
    "position_book_near_10_imbalance",
    "position_book_near_25_imbalance",
    "position_book_near_50_imbalance",
    "position_book_above_25_net",
    "position_book_below_25_net",
    "position_book_long_peak_distance_pips",
    "position_book_short_peak_distance_pips",
    "position_book_concentration",
)


FAMILIES = (
    "momentum",
    "ahl_multihorizon_trend",
    "pullback",
    "macd_rsi_reversal",
    "ema_trend_cross",
    "bollinger_reversion",
    "donchian_breakout",
    "currency_strength",
    "relative_value_reversion",
    "supervised_return_rank",
    "higher_timeframe_alignment",
    "rsi_trend_continuation",
    "stochastic_reversal",
    "volatility_squeeze_breakout",
    "range_expansion",
    "candlestick_reversal",
    "linear_regression_trend",
    "volume_impulse",
    "atr_mean_reversion",
    "cross_sectional_pair_rank",
    "regime_switching",
    "pattern_count_forecast",
    "failed_breakout_reversal",
    "efficiency_filtered_momentum",
    "cross_pair_lead_lag",
    "spread_compression_momentum",
    "kama_adaptive_trend",
    "cci_reversion",
    "breakout_retest",
    "volume_climax_reversal",
    "spread_mean_reversion",
    "volatility_shock_fade",
    "session_range_breakout",
    "micro_channel_break",
    "trend_momentum_confluence",
    "breakout_volume_confluence",
    "oscillator_reversion_confluence",
    "cross_market_confluence",
    "inverse_correlation_veto",
    "signal_combination_rules",
    "kalman_local_trend",
    "markov_sign_transition",
    "online_ar_forecast",
    "variance_ratio_regime",
    "permutation_entropy_momentum",
    "cusum_breakout",
    "har_volatility_momentum",
    "bipower_jump_reversal",
    "garch_volatility_breakout",
    "hurst_regime_forecast",
    "theil_sen_trend",
    "vwap_deviation_reversion",
    "ny_session_vwap_sell_reversion",
)
DEFAULT_FAMILIES = (
    "momentum",
    "pullback",
    "macd_rsi_reversal",
    "ema_trend_cross",
    "bollinger_reversion",
    "donchian_breakout",
    "currency_strength",
    "relative_value_reversion",
    "supervised_return_rank",
    "higher_timeframe_alignment",
    "rsi_trend_continuation",
    "stochastic_reversal",
    "volatility_squeeze_breakout",
    "range_expansion",
    "candlestick_reversal",
    "linear_regression_trend",
    "volume_impulse",
    "atr_mean_reversion",
    "cross_sectional_pair_rank",
    "regime_switching",
    "pattern_count_forecast",
    "failed_breakout_reversal",
    "efficiency_filtered_momentum",
    "cross_pair_lead_lag",
    "spread_compression_momentum",
    "kama_adaptive_trend",
    "cci_reversion",
    "breakout_retest",
    "volume_climax_reversal",
    "spread_mean_reversion",
    "volatility_shock_fade",
    "session_range_breakout",
    "micro_channel_break",
    "trend_momentum_confluence",
    "breakout_volume_confluence",
    "oscillator_reversion_confluence",
    "cross_market_confluence",
    "inverse_correlation_veto",
    "signal_combination_rules",
    "kalman_local_trend",
    "markov_sign_transition",
    "online_ar_forecast",
    "variance_ratio_regime",
    "permutation_entropy_momentum",
    "cusum_breakout",
    "har_volatility_momentum",
    "bipower_jump_reversal",
    "garch_volatility_breakout",
    "hurst_regime_forecast",
    "theil_sen_trend",
    "vwap_deviation_reversion",
    "ny_session_vwap_sell_reversion",
)
PROFILES = ("strict", "balanced", "fast", "loose")
FULL_HORIZONS_SEC = (
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
META_FAMILIES = {"inverse_correlation_veto", "signal_combination_rules"}
AGGREGATE_SIGNAL_LINEAGE_CONTRACT_ID = "all_signal_horizon_lineage_v1"
DIRECTION_CONFLICT_CONTRACT_ID = "any_opposing_preferred_horizon_contributor_v1"
RESEARCH_SHADOW_FAMILIES = {
    "ahl_multihorizon_trend",
    "kalman_local_trend",
    "markov_sign_transition",
    "online_ar_forecast",
    "variance_ratio_regime",
    "permutation_entropy_momentum",
    "cusum_breakout",
    "har_volatility_momentum",
    "bipower_jump_reversal",
    "garch_volatility_breakout",
    "hurst_regime_forecast",
    "theil_sen_trend",
    "vwap_deviation_reversion",
    "ny_session_vwap_sell_reversion",
}
INVERSE_VOTE_SOURCES = (
    "momentum",
    "efficiency_filtered_momentum",
    "linear_regression_trend",
    "trend_momentum_confluence",
    "cross_pair_lead_lag",
)
COOLED_LOOSE_ACCOUNT_FAMILIES = {
    "momentum",
    "efficiency_filtered_momentum",
    "linear_regression_trend",
    "trend_momentum_confluence",
    "cross_pair_lead_lag",
    "cross_market_confluence",
    "currency_strength",
    "breakout_volume_confluence",
    "range_expansion",
}
LOG_LOCK = threading.Lock()
LOG_HANDLES: dict[Path, Any] = {}
LOG_LAST_FLUSH: dict[Path, float] = {}
LOG_BYTES: dict[Path, int] = {}
LOG_ROTATION_SEQUENCE: dict[Path, int] = {}
LOG_NEXT_ROTATION_ATTEMPT: dict[Path, float] = {}
LOG_EVENT_COUNTS: dict[Path, dict[str, int]] = {}
SUPERVISED_MODEL_CACHE: dict[str, Any] = {}
OUTCOME_QUOTE_MAX_AGE_SEC = 30.0
CONVERSION_QUOTE_MAX_AGE_SEC = 15.0
OUTCOME_QUOTE_GRACE_SEC = 30.0
# A strategy-lab pass can take 70-90 seconds. Refresh the read-only pricing
# overlay comfortably inside the unchanged 30-second outcome/forecast gate.
# Refresh cadence and acceptance freshness are intentionally separate.
LANE_QUOTE_REFRESH_INTERVAL_SEC = min(10.0, OUTCOME_QUOTE_MAX_AGE_SEC / 3.0)


def forex_market_open_at(value: datetime) -> bool:
    local = value.astimezone(NEW_YORK_TZ)
    weekday = local.weekday()
    if weekday == 5:
        return False
    if weekday == 4 and local.hour >= 17:
        return False
    if weekday == 6 and local.hour < 17:
        return False
    return True


def execution_horizon_market_target(candidate: dict[str, Any]) -> dict[str, Any]:
    origin = parse_rfc3339(
        str(candidate.get("entry_time") or candidate.get("observed_at") or "")
    )
    if origin is None:
        origin = datetime.now(timezone.utc)
    horizon = max(
        0,
        int(
            safe_float(
                candidate.get("execution_horizon_sec"),
                safe_float(candidate.get("preferred_horizon_sec")),
            )
        ),
    )
    target = origin + timedelta(seconds=horizon)
    return {
        "origin_utc": origin.astimezone(timezone.utc).isoformat(),
        "target_utc": target.astimezone(timezone.utc).isoformat(),
        "market_open": forex_market_open_at(target),
        "policy": "nominal_horizon_endpoint_must_be_tradeable",
    }


@dataclass(frozen=True)
class LaneSpec:
    lane_id: str
    family: str
    profile: str
    parameters: dict[str, Any]


@dataclass(frozen=True)
class RawSetup:
    direction: str | None
    reason: str
    signal: dict[str, Any]


class MarketDataClient:
    """OANDA client with read-heavy sessions and non-retried practice writes."""

    def __init__(self, token: str, *, timeout_sec: float = 20.0, retries: int = 3) -> None:
        self.token = token
        self.timeout_sec = timeout_sec
        self.retries = retries
        self._thread_local = threading.local()
        self._pricing_session = self._new_session()

    def _new_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Bearer {self.token}",
                "Accept-Datetime-Format": "RFC3339",
            }
        )
        return session

    def _session(self, pricing: bool) -> requests.Session:
        if pricing:
            return self._pricing_session
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = self._new_session()
            self._thread_local.session = session
        return session

    def _reset_session(self, pricing: bool) -> None:
        if pricing:
            self._pricing_session.close()
            self._pricing_session = self._new_session()
            return
        session = getattr(self._thread_local, "session", None)
        if session is not None:
            session.close()
        self._thread_local.session = self._new_session()

    def get(self, path: str, *, params: dict[str, str], pricing: bool = False) -> dict[str, Any]:
        last_error: BaseException | None = None
        for attempt in range(self.retries):
            try:
                response = self._session(pricing).get(
                    f"{BASE_URL}{path}",
                    params=params,
                    timeout=self.timeout_sec,
                )
            except requests.RequestException as exc:
                last_error = exc
                self._reset_session(pricing)
                if attempt + 1 < self.retries:
                    time.sleep(0.25 * (2**attempt))
                    continue
                raise OandaApiError(
                    f"OANDA market-data request failed: {type(exc).__name__}",
                    method="GET",
                    endpoint=path,
                ) from exc
            if response.status_code < 400:
                try:
                    payload = response.json()
                except ValueError as exc:
                    raise OandaApiError(
                        "OANDA market-data response was not JSON",
                        status=response.status_code,
                        method="GET",
                        endpoint=path,
                    ) from exc
                return payload if isinstance(payload, dict) else {}
            retryable = response.status_code == 429 or response.status_code >= 500
            if retryable and attempt + 1 < self.retries:
                retry_after = safe_float(response.headers.get("Retry-After"), 0.25 * (2**attempt))
                time.sleep(min(5.0, max(0.1, retry_after)))
                continue
            raise OandaApiError(
                f"OANDA market-data GET failed with HTTP {response.status_code}: {response.text[:300]}",
                status=response.status_code,
                method="GET",
                endpoint=path,
                request_id=str(response.headers.get("RequestID") or ""),
            )
        raise OandaApiError(
            f"OANDA market-data GET failed: {type(last_error).__name__}",
            method="GET",
            endpoint=path,
        )

    def pricing_snapshot(self, account_id: str, instruments: list[str]) -> dict[str, Any]:
        prices: dict[str, Any] = {}
        for index in range(0, len(instruments), 50):
            chunk = instruments[index : index + 50]
            payload = self.get(
                f"/v3/accounts/{account_id}/pricing",
                params={"instruments": ",".join(chunk), "includeUnitsAvailable": "false"},
                pricing=True,
            )
            for price in payload.get("prices") or []:
                instrument = str(price.get("instrument") or "")
                if instrument:
                    prices[instrument] = quote_from_price_payload(price, "lab_pricing")
        return prices

    def complete_candles(self, instrument: str, granularity: str, count: int) -> list[dict[str, Any]]:
        payload = self.get(
            f"/v3/instruments/{instrument}/candles",
            params={"price": "MBA", "granularity": granularity, "count": str(count)},
        )
        candles = payload.get("candles") or []
        return [candle for candle in candles if isinstance(candle, dict) and candle.get("complete")]

    def write(
        self,
        method: str,
        path: str,
        body: dict[str, Any],
        *,
        client_request_id: str = "",
    ) -> dict[str, Any]:
        headers = {"X-Request-ID": client_request_id} if client_request_id else None
        try:
            response = self._session(False).request(
                method.upper(),
                f"{BASE_URL}{path}",
                json=body,
                headers=headers,
                timeout=self.timeout_sec,
            )
        except requests.RequestException as exc:
            self._reset_session(False)
            raise OandaApiError(
                f"OANDA practice write failed: {type(exc).__name__}",
                method=method.upper(),
                endpoint=path,
                outcome_uncertain=True,
            ) from exc
        request_id = str(response.headers.get("RequestID") or "")
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if response.status_code >= 400:
            raise OandaApiError(
                f"OANDA practice write failed with HTTP {response.status_code}: {response.text[:300]}",
                status=response.status_code,
                method=method.upper(),
                endpoint=path,
                request_id=request_id,
                payload=payload,
            )
        if isinstance(payload, dict):
            payload["_requestID"] = request_id
            return payload
        return {"_requestID": request_id}

    def close(self) -> None:
        self._pricing_session.close()


class LanePerformance:
    def __init__(self, horizon_sec: int = 300, max_samples: int = 100) -> None:
        self.horizon_sec = int(horizon_sec)
        self.samples: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=max_samples))

    def observe(self, lane_id: str, horizon_sec: float, pips: float, kind: str) -> None:
        if kind != "signal" or int(horizon_sec) != self.horizon_sec:
            return
        if lane_id and math.isfinite(pips):
            self.samples[lane_id].append(float(pips))

    def load(self, path: Path) -> int:
        loaded = 0
        if not path.is_file():
            return loaded
        with path.open("r", encoding="utf-8-sig") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if (
                    row.get("event") != "shadow_outcome"
                    or row.get("kind") != "signal"
                    or int(safe_float(row.get("horizon_sec"))) != self.horizon_sec
                ):
                    continue
                self.observe(
                    str(row.get("lane_id") or ""),
                    safe_float(row.get("horizon_sec")),
                    safe_float(row.get("theoretical_pips")),
                    str(row.get("kind") or ""),
                )
                loaded += 1
        return loaded

    def stats(self, lane_id: str) -> dict[str, Any]:
        values = list(self.samples.get(lane_id) or [])
        if not values:
            return {
                "lane_id": lane_id, "n": 0, "avg": 0.0, "median": 0.0,
                "win_rate": 0.0, "stdev": 0.0, "lower_confidence": -math.inf, "score": -math.inf,
            }
        average = statistics.fmean(values)
        stdev = statistics.stdev(values) if len(values) > 1 else math.inf
        lower_confidence = average - 1.645 * stdev / math.sqrt(len(values)) if math.isfinite(stdev) else -math.inf
        return {
            "lane_id": lane_id,
            "n": len(values),
            "avg": round(average, 4),
            "median": round(statistics.median(values), 4),
            "win_rate": round(100.0 * sum(value > 0.0 for value in values) / len(values), 1),
            "stdev": round(stdev, 4) if math.isfinite(stdev) else math.inf,
            "lower_confidence": round(lower_confidence, 4),
            "score": round(lower_confidence, 4),
        }

    def top(self, min_samples: int, limit: int) -> list[dict[str, Any]]:
        eligible = [self.stats(lane_id) for lane_id in self.samples]
        eligible = [row for row in eligible if row["n"] >= min_samples]
        eligible.sort(key=lambda row: (row["score"], row["n"], row["lane_id"]), reverse=True)
        return eligible[:limit]

    def recent_negative_veto(
        self,
        lane_id: str,
        *,
        min_samples: int = 5,
        max_win_rate: float = 40.0,
    ) -> dict[str, Any]:
        """Block a lane only when several direct live outcomes are uniformly poor."""

        evidence = self.stats(lane_id)
        veto = bool(
            int(evidence["n"]) >= max(1, int(min_samples))
            and safe_float(evidence["avg"]) < 0.0
            and safe_float(evidence["median"]) <= 0.0
            and safe_float(evidence["win_rate"]) <= float(max_win_rate)
            and safe_float(evidence["lower_confidence"], -math.inf) < 0.0
        )
        positive_ready = bool(
            int(evidence["n"]) >= max(1, int(min_samples))
            and safe_float(evidence["avg"]) > 0.0
            and safe_float(evidence["median"]) >= 0.0
            and safe_float(evidence["win_rate"]) >= 50.0
        )
        return {**evidence, "veto": veto, "positive_ready": positive_ready}


class SharedSignalFeed(SignalContributionFeed):
    """Compatibility name for the canonical cross-process contribution feed."""


def partial_shadow_lane_threshold(total_lanes: int) -> int:
    """Publish before a long cycle can outrun the feed's 90-second TTL.

    A one-third checkpoint was usually early enough, but a measured 118-second
    cycle put it fractionally after the prior generation expired.  One quarter
    keeps a useful safety margin while the rows remain forced research-only.
    """

    return max(1, int(total_lanes) // 4)


class AsyncSignalFeedCache:
    """Refresh a read-only signal-feed cache without blocking trade management."""

    def __init__(
        self,
        path: Path,
        *,
        limit: int,
        refresh_sec: float,
        ttl_sec: float,
        feed_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.path = Path(path)
        self.limit = max(1, int(limit))
        self.refresh_sec = max(0.1, float(refresh_sec))
        self.ttl_sec = max(1.0, float(ttl_sec))
        self._feed_factory = feed_factory
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._rows: list[dict[str, Any]] = []
        self._last_success_monotonic: float | None = None
        self._last_success_epoch: float | None = None
        self._last_attempt_epoch: float | None = None
        self._last_error = ""
        self._last_refresh_ms: float | None = None
        self._refreshing = False
        self._generation = 0

    def _new_feed(self) -> Any:
        if self._feed_factory is not None:
            return self._feed_factory()
        feed = SharedSignalFeed(
            self.path,
            initialize_schema=False,
            register_model_gap_inventory=False,
        )
        feed.connection.execute("PRAGMA query_only=ON")
        return feed

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="practice-007-async-signal-feed-cache",
            daemon=True,
        )
        self._thread.start()

    def _run(self) -> None:
        feed: Any = None
        try:
            feed = self._new_feed()
            while not self._stop.is_set():
                started = time.perf_counter()
                with self._lock:
                    self._refreshing = True
                    self._last_attempt_epoch = time.time()
                try:
                    rows = [
                        dict(row)
                        for row in feed.recent(self.limit)
                        if isinstance(row, dict)
                    ]
                    finished_monotonic = time.monotonic()
                    finished_epoch = time.time()
                    with self._lock:
                        self._rows = rows
                        self._last_success_monotonic = finished_monotonic
                        self._last_success_epoch = finished_epoch
                        self._last_error = ""
                        self._last_refresh_ms = round(
                            (time.perf_counter() - started) * 1000.0,
                            3,
                        )
                        self._generation += 1
                except Exception as exc:
                    try:
                        feed.connection.rollback()
                    except Exception:
                        pass
                    with self._lock:
                        self._last_error = (
                            f"{type(exc).__name__}: {exc}"[:500]
                        )
                        self._last_refresh_ms = round(
                            (time.perf_counter() - started) * 1000.0,
                            3,
                        )
                finally:
                    with self._lock:
                        self._refreshing = False
                self._stop.wait(self.refresh_sec)
        finally:
            if feed is not None:
                try:
                    feed.close()
                except Exception:
                    pass

    def snapshot(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        now_monotonic = time.monotonic()
        now_epoch = time.time()
        with self._lock:
            rows = [dict(row) for row in self._rows]
            success_monotonic = self._last_success_monotonic
            success_epoch = self._last_success_epoch
            attempt_epoch = self._last_attempt_epoch
            error = self._last_error
            refresh_ms = self._last_refresh_ms
            refreshing = self._refreshing
            generation = self._generation
        cache_age_sec = (
            math.inf
            if success_monotonic is None
            else max(0.0, now_monotonic - success_monotonic)
        )
        fresh_rows = [
            row
            for row in rows
            if (
                safe_float(row.get("feed_published_epoch"), now_epoch)
                >= now_epoch - self.ttl_sec
            )
        ]
        source_counts: dict[str, int] = {}
        source_generations: dict[str, set[float]] = {}
        source_newest_epoch: dict[str, float] = {}
        semantic_keys: set[tuple[str, ...]] = set()
        semantic_rows = 0
        for row in fresh_rows:
            source = str(
                row.get("feed_source") or row.get("source") or "unknown"
            )
            source_counts[source] = source_counts.get(source, 0) + 1
            published_epoch = safe_float(
                row.get("feed_published_epoch"),
                now_epoch,
            )
            source_generations.setdefault(source, set()).add(
                round(published_epoch, 3)
            )
            source_newest_epoch[source] = max(
                source_newest_epoch.get(source, -math.inf),
                published_epoch,
            )
            semantic_key = (
                source,
                str(row.get("instrument") or ""),
                str(row.get("direction") or row.get("side") or ""),
                str(row.get("family") or ""),
                str(row.get("input_timeframe") or ""),
                str(row.get("model_id") or ""),
                str(row.get("signal_reference_horizon_sec") or ""),
            )
            if any(semantic_key[1:]):
                semantic_rows += 1
                semantic_keys.add(semantic_key)
        source_generation_counts = {
            source: len(generations)
            for source, generations in sorted(source_generations.items())
        }
        source_newest_age_sec = {
            source: round(max(0.0, now_epoch - epoch), 3)
            for source, epoch in sorted(source_newest_epoch.items())
        }
        source_data_stale = bool(rows) and not fresh_rows
        fresh = bool(
            success_monotonic is not None
            and cache_age_sec <= self.ttl_sec
            and not source_data_stale
        )
        state = (
            "warming"
            if success_monotonic is None and not error
            else "error"
            if success_monotonic is None and error
            else "stale"
            if not fresh
            else "fresh"
        )
        return (
            fresh_rows if fresh else [],
            {
                "state": state,
                "fresh": fresh,
                "refreshing": refreshing,
                "generation": generation,
                "candidate_count": len(fresh_rows) if fresh else 0,
                "raw_candidate_count": len(rows),
                "expired_candidate_count": len(rows) - len(fresh_rows),
                "source_counts": dict(sorted(source_counts.items())),
                "source_generation_counts": source_generation_counts,
                "source_newest_age_sec": source_newest_age_sec,
                "semantic_duplicate_rows": max(
                    0,
                    semantic_rows - len(semantic_keys),
                ),
                "cache_age_sec": (
                    None
                    if not math.isfinite(cache_age_sec)
                    else round(cache_age_sec, 3)
                ),
                "ttl_sec": self.ttl_sec,
                "refresh_sec": self.refresh_sec,
                "last_refresh_ms": refresh_ms,
                "last_success_epoch": success_epoch,
                "last_attempt_epoch": attempt_epoch,
                "last_error": error,
                "source_data_stale": source_data_stale,
                "fail_closed": not fresh,
            },
        )

    def stop(self, join_timeout_sec: float = 2.0) -> bool:
        self._stop.set()
        thread = self._thread
        if thread is None:
            return True
        thread.join(timeout=max(0.0, float(join_timeout_sec)))
        return not thread.is_alive()


class AsyncJsonSnapshotWriter:
    """Coalesce JSON snapshots and keep slow Windows replaces off the hot loop."""

    def __init__(
        self,
        path: Path,
        *,
        writer: Callable[[Path, dict[str, Any]], None] | None = None,
    ) -> None:
        self.path = Path(path)
        self._writer = writer or atomic_json
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._payload: dict[str, Any] | None = None
        self._submitted_generation = 0
        self._written_generation = 0
        self._last_submit_epoch: float | None = None
        self._last_success_epoch: float | None = None
        self._last_write_ms: float | None = None
        self._last_error = ""
        self._writing = False

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="practice-007-async-signal-snapshot",
            daemon=True,
        )
        self._thread.start()

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            self._payload = payload
            self._submitted_generation += 1
            self._last_submit_epoch = time.time()
        self._wake.set()
        return self.status()

    def _run(self) -> None:
        while not self._stop.is_set():
            self._wake.wait(0.5)
            if self._stop.is_set():
                break
            if not self._wake.is_set():
                continue
            self._wake.clear()
            with self._lock:
                payload = self._payload
                generation = self._submitted_generation
                self._writing = payload is not None
            if payload is None:
                continue
            started = time.perf_counter()
            error = ""
            try:
                self._writer(self.path, payload)
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"[:500]
            finished_epoch = time.time()
            write_ms = round((time.perf_counter() - started) * 1000.0, 3)
            with self._lock:
                self._writing = False
                self._last_write_ms = write_ms
                self._last_error = error
                if not error:
                    self._written_generation = max(
                        self._written_generation,
                        generation,
                    )
                    self._last_success_epoch = finished_epoch

    def status(self) -> dict[str, Any]:
        with self._lock:
            submitted = self._submitted_generation
            written = self._written_generation
            return {
                "enabled": True,
                "writing": self._writing,
                "pending": submitted > written,
                "submitted_generation": submitted,
                "written_generation": written,
                "coalesced_generation_count": max(0, submitted - written - 1),
                "last_submit_epoch": self._last_submit_epoch,
                "last_success_epoch": self._last_success_epoch,
                "last_write_ms": self._last_write_ms,
                "last_error": self._last_error,
            }

    def stop(self, join_timeout_sec: float = 2.0) -> bool:
        self._stop.set()
        self._wake.set()
        thread = self._thread
        if thread is None:
            return True
        thread.join(timeout=max(0.0, float(join_timeout_sec)))
        return not thread.is_alive()


def price_precision(pip: float) -> int:
    return max(0, int(round(-math.log10(max(pip, 1e-12)))) + 1)


def normalized_trailing_distance(
    configured_pips: float,
    pip: float,
    minimum_price_distance: float,
    precision: int,
) -> tuple[float, float]:
    pip = max(1e-12, safe_float(pip))
    requested = max(0.0, safe_float(configured_pips)) * pip
    minimum = max(0.0, safe_float(minimum_price_distance))
    scale = 10.0 ** max(0, int(precision))
    distance = math.ceil((max(requested, minimum) * scale) - 1e-10) / scale
    return distance, distance / pip


def profit_lock_stop_price(
    direction: str,
    entry_price: float,
    current_pips: float,
    pip: float,
    trailing_distance_pips: float,
    trigger_pips: float,
    floor_pips: float,
    spread_pips: float,
    spread_multiple: float,
    existing_stop_price: float | None,
    precision: int,
    minimum_step_pips: float,
) -> dict[str, float] | None:
    """Return a tighter broker stop that locks profit without crossing the market."""

    side = str(direction or "").lower()
    if side not in {"buy", "sell"}:
        return None
    pip = max(1e-12, safe_float(pip))
    current = safe_float(current_pips)
    floor = max(0.0, safe_float(floor_pips))
    market_buffer = max(
        0.5,
        max(0.0, safe_float(spread_pips))
        * max(0.0, safe_float(spread_multiple)),
    )
    if current < max(safe_float(trigger_pips), floor + market_buffer):
        return None
    desired_lock = min(
        current - market_buffer,
        max(floor, current - max(0.0, safe_float(trailing_distance_pips))),
    )
    if desired_lock < floor:
        return None

    sign = 1.0 if side == "buy" else -1.0
    existing_price = safe_float(existing_stop_price)
    existing_lock = (
        sign * (existing_price - safe_float(entry_price)) / pip
        if existing_price > 0.0
        else -math.inf
    )
    step = max(0.0, safe_float(minimum_step_pips))
    if desired_lock <= existing_lock + step:
        return None

    scale = 10.0 ** max(0, int(precision))
    raw_price = safe_float(entry_price) + sign * desired_lock * pip
    if side == "buy":
        stop_price = math.floor(raw_price * scale + 1e-10) / scale
    else:
        stop_price = math.ceil(raw_price * scale - 1e-10) / scale
    actual_lock = sign * (stop_price - safe_float(entry_price)) / pip
    if actual_lock <= existing_lock + step or actual_lock < floor - 1e-9:
        return None
    return {
        "price": stop_price,
        "lock_pips": actual_lock,
        "existing_lock_pips": existing_lock,
        "market_buffer_pips": market_buffer,
    }


def build_practice_order(
    candidate: dict[str, Any],
    units: int,
    max_slippage_pips: float,
    client_id: str,
) -> dict[str, Any]:
    direction = str(candidate["direction"])
    pip = safe_float(candidate["pip"], 0.0001)
    precision = price_precision(pip)
    signed_units = abs(int(units)) if direction == "buy" else -abs(int(units))
    entry = safe_float(candidate["ask"] if direction == "buy" else candidate["bid"])
    stop_pips = safe_float(candidate["stop_loss_pips"], 5.0)
    target_pips = stop_pips * safe_float(candidate["take_profit_r"], 1.5)
    sign = 1.0 if direction == "buy" else -1.0
    horizon_sec = int(
        safe_float(
            candidate.get("execution_exit_horizon_sec"),
            safe_float(candidate.get("execution_horizon_sec")),
        )
    )
    comment = str(candidate["lane_id"])
    if horizon_sec > 0:
        comment = f"{comment}|h{horizon_sec}"
    trailing_activation = safe_float(candidate.get("trailing_activation_pips"))
    trailing_distance = safe_float(candidate.get("trailing_distance_pips"))
    confidence = safe_float(candidate.get("signal_confidence"))
    if trailing_activation > 0.0 and trailing_distance > 0.0:
        comment = f"{comment}|ta{trailing_activation:.2f}|td{trailing_distance:.2f}"
    if confidence > 0.0:
        comment = f"{comment}|c{confidence:.3f}"
    order = {
        "type": "MARKET",
        "instrument": str(candidate["instrument"]),
        "units": str(signed_units),
        "timeInForce": "FOK",
        "positionFill": "OPEN_ONLY",
        "priceBound": f"{entry + sign * max_slippage_pips * pip:.{precision}f}",
        "stopLossOnFill": {
            "price": f"{entry - sign * stop_pips * pip:.{precision}f}",
            "timeInForce": "GTC",
        },
        "clientExtensions": {
            "id": client_id,
            "tag": "strategy_lab_top",
            "comment": comment[:120],
        },
        "tradeClientExtensions": {
            "id": client_id,
            "tag": "strategy_lab_top",
            "comment": comment[:120],
        },
    }
    if not bool(candidate.get("open_ended_profit")):
        order["takeProfitOnFill"] = {
            "price": f"{entry + sign * target_pips * pip:.{precision}f}",
            "timeInForce": "GTC",
        }
    return {"order": order}


class PracticeExecutor:
    def __init__(
        self,
        client: MarketDataClient,
        account_id: str,
        log_path: Path,
        performance: LanePerformance,
        args: argparse.Namespace,
        exit_fit: StrategyExitFit | None = None,
        promotion: LanePromotionModel | None = None,
    ) -> None:
        self.client = client
        self.account_id = account_id
        self.log_path = log_path
        self.performance = performance
        self.args = args
        self.exit_fit = exit_fit
        self.promotion = promotion
        policy_path = getattr(args, "execution_policy_state", None)
        self.execution_policy = (
            FrozenExecutionPolicy(Path(policy_path)) if policy_path else None
        )
        feed_path = getattr(args, "execution_signal_feed_database", None)
        feed_consumer_only = bool(
            getattr(args, "execution_feed_consumer_only", False)
        )
        self.signal_feed = (
            SharedSignalFeed(
                Path(feed_path),
                initialize_schema=not feed_consumer_only,
                register_model_gap_inventory=not feed_consumer_only,
            )
            if feed_path
            else None
        )
        self.signal_feed_cache = (
            self._build_async_signal_feed_cache(Path(feed_path))
            if feed_path and feed_consumer_only
            else None
        )
        self.last_feed_cache_stats: dict[str, Any] = {
            "state": "disabled" if self.signal_feed_cache is None else "warming",
            "fresh": False,
            "fail_closed": self.signal_feed_cache is not None,
        }
        self.last_feed_cache_state = ""
        snapshot_path = getattr(args, "execution_signal_snapshot", None)
        self.signal_snapshot_writer = (
            AsyncJsonSnapshotWriter(Path(snapshot_path))
            if snapshot_path and feed_consumer_only
            else None
        )
        self.last_signal_snapshot_writer_stats: dict[str, Any] = {
            "enabled": self.signal_snapshot_writer is not None,
            "pending": False,
        }
        self.last_qualified_candidates: list[dict[str, Any]] = []
        self.last_feed_candidate_count = 0
        self.last_execution_prefilter_count = 0
        self.price_snapshot_provider: Callable[[], dict[str, Any]] | None = None
        self.start_balance = 0.0
        self.account_currency = "USD"
        self.instrument_meta: dict[str, dict[str, float]] = {}
        self.conversion_cache: dict[str, tuple[float, float]] = {}
        self.last_entry_monotonic = -math.inf
        self.next_manage_monotonic = 0.0
        self.last_selection_log_monotonic = -math.inf
        self.last_execution_skip_log_monotonic = -math.inf
        self.last_execution_skip_reason = ""
        self.last_execution_selection_blocks: list[dict[str, Any]] = []
        self.last_signal_snapshot_monotonic = -math.inf
        self.last_feed_coverage_monotonic = -math.inf
        self.feed_coverage_cache: dict[str, Any] = {}
        self.last_rank_timings: dict[str, float] = {}
        self.last_signal_snapshot_timings: dict[str, Any] = {}
        self.second_curve_cache_at = -math.inf
        self.second_curve_cache_mtime_ns = -1
        self.second_curve_cache: dict[str, list[dict[str, Any]]] = {}
        self.fills = 0
        self.disabled_reason = ""
        self.has_owned_trade = False
        self.owned_trade_state: dict[str, dict[str, Any]] = {}
        self.recent_pair_direction_exits: dict[str, float] = {}
        self.recent_instrument_exits: dict[str, float] = {}
        self.recent_factor_exits: dict[str, float] = {}
        self.reentry_state_path = self.log_path.parent / (
            f"practice_executor_reentry_{self.account_id[-4:]}.json"
        )
        self.order_telemetry: deque[dict[str, Any]] = deque(maxlen=200)
        self.last_order_telemetry: dict[str, Any] = {}

    def _build_async_signal_feed_cache(self, path: Path) -> AsyncSignalFeedCache:
        cache_ttl_sec = safe_float(
            getattr(self.args, "execution_signal_feed_cache_ttl_sec", None),
            safe_float(
                getattr(self.args, "execution_signal_feed_ttl_sec", 90.0),
                90.0,
            ),
        )
        return AsyncSignalFeedCache(
            Path(path),
            limit=int(
                safe_float(
                    getattr(
                        self.args,
                        "execution_signal_feed_limit",
                        50_000,
                    ),
                    50_000,
                )
            ),
            refresh_sec=safe_float(
                getattr(
                    self.args,
                    "execution_signal_feed_cache_refresh_sec",
                    5.0,
                ),
                5.0,
            ),
            ttl_sec=cache_ttl_sec,
        )

    def ensure_async_signal_feed_cache(self) -> bool:
        """Recover cache wiring when a launcher binds its feed after construction."""
        feed_path = getattr(self.args, "execution_signal_feed_database", None)
        if (
            self.signal_feed_cache is not None
            or not feed_path
            or not bool(
                getattr(self.args, "execution_feed_consumer_only", False)
            )
        ):
            return False
        self.signal_feed_cache = self._build_async_signal_feed_cache(
            Path(feed_path)
        )
        self.last_feed_cache_stats = {
            "state": "warming",
            "fresh": False,
            "fail_closed": True,
        }
        return True

    def set_price_snapshot_provider(
        self,
        provider: Callable[[], dict[str, Any]] | None,
    ) -> None:
        self.price_snapshot_provider = provider

    def refresh_candidate_quotes(
        self,
        candidates: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Reprice candle-derived signals immediately before account ranking."""
        if self.price_snapshot_provider is None:
            return [dict(candidate) for candidate in candidates]
        try:
            prices = self.price_snapshot_provider()
        except Exception as exc:
            log_line(
                self.log_path,
                "execution_quote_refresh_error",
                error=f"{type(exc).__name__}: {exc}"[:300],
            )
            return [dict(candidate) for candidate in candidates]
        refreshed: list[dict[str, Any]] = []
        refreshed_at = time.monotonic()
        for candidate in candidates:
            row = dict(candidate)
            quote = prices.get(str(row.get("instrument") or ""))
            if quote is None or outcome_quote_rejection_reason(quote):
                refreshed.append(row)
                continue
            pip = max(1e-12, safe_float(row.get("pip")))
            spread = max(0.0, (safe_float(quote.ask) - safe_float(quote.bid)) / pip)
            signal_strength = abs(safe_float(row.get("signal_strength_pips")))
            target = (
                safe_float(row.get("stop_loss_pips"))
                * safe_float(row.get("take_profit_r"))
            )
            row.update(
                {
                    "bid": safe_float(quote.bid),
                    "ask": safe_float(quote.ask),
                    "entry_time": str(quote.time or ""),
                    "spread_pips": round(spread, 3),
                    "signal_to_spread": (
                        math.inf if spread <= 0.0 else signal_strength / spread
                    ),
                    "reward_to_spread": (
                        math.inf if spread <= 0.0 else target / spread
                    ),
                    "forecast_created_monotonic": refreshed_at,
                }
            )
            refreshed.append(row)
        return refreshed

    @staticmethod
    def merge_signal_candidates(
        local_candidates: list[dict[str, Any]],
        shared_candidates: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Merge feeds while keeping the current process's fresh copy on collision."""
        merged: dict[str, dict[str, Any]] = {}
        anonymous: list[dict[str, Any]] = []
        for candidate in [*shared_candidates, *local_candidates]:
            candidate_id = str(candidate.get("id") or "")
            if candidate_id:
                merged[candidate_id] = candidate
            else:
                anonymous.append(candidate)
        return [*merged.values(), *anonymous]

    @classmethod
    def execution_candidate_prefilter(
        cls,
        candidates: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Keep full evidence only where an executable structural seed exists.

        Research-only, timing, correlation, and opposing rows remain available
        for consensus and vetoes on a seeded instrument.  Instruments with no
        accepted account-eligible structural candidate cannot produce an
        order, so ranking their full evidence surface on the fast executor is
        unnecessary.
        """

        seeded_instruments = {
            str(row.get("instrument") or "")
            for row in candidates
            if str(row.get("instrument") or "")
            and bool(row.get("account_eligible", False))
            and not bool(row.get("research_only", False))
            and str(row.get("signal_role") or "structural")
            != "entry_exit_timing"
            and str(row.get("preconsensus_class") or "accepted")
            == "accepted"
        }
        if not seeded_instruments:
            return []
        # The shared feed can contain several 90-second generations of the
        # same component. Re-ranking each copy inflates consensus work and can
        # stall entry selection during producer bursts. Keep the newest copy
        # of each semantic component; distinct models, lanes, timeframes,
        # roles, profiles, directions, and source kinds remain independent.
        latest: dict[tuple[str, ...], tuple[float, tuple[Any, ...], dict[str, Any]]] = {}
        for row in candidates:
            if str(row.get("source_kind") or "") == "partial_cycle_shadow":
                continue
            instrument = str(row.get("instrument") or "")
            if instrument not in seeded_instruments:
                continue
            source = str(
                row.get("model_id")
                or row.get("lane_id")
                or row.get("family")
                or row.get("id")
                or "anonymous"
            )
            key = (
                instrument,
                str(row.get("direction") or ""),
                source,
                str(row.get("input_timeframe") or "multi"),
                str(row.get("signal_role") or "structural"),
                str(row.get("profile") or ""),
                str(row.get("source_kind") or "strategy_signal"),
            )
            published = safe_float(row.get("feed_published_epoch"), 0.0)
            rank_key = cls._signal_rank_key(row)
            current = latest.get(key)
            if current is None or (published, rank_key) > (current[0], current[1]):
                latest[key] = (published, rank_key, row)
        return [value[2] for value in latest.values()]

    def initialize(self) -> None:
        if "practice" not in BASE_URL.lower():
            raise SystemExit("Top-signal execution is restricted to the OANDA practice endpoint.")
        summary = self.client.get(f"/v3/accounts/{self.account_id}/summary", params={}).get("account") or {}
        self.start_balance = safe_float(summary.get("balance"))
        self.account_currency = str(summary.get("currency") or "USD")
        instrument_payload = self.client.get(f"/v3/accounts/{self.account_id}/instruments", params={})
        for row in instrument_payload.get("instruments") or []:
            instrument = str(row.get("name") or "")
            if not instrument:
                continue
            self.instrument_meta[instrument] = {
                "margin_rate": max(1e-6, safe_float(row.get("marginRate"), safe_float(summary.get("marginRate"), 0.02))),
                "minimum_trade_size": max(1.0, safe_float(row.get("minimumTradeSize"), 1.0)),
                "pip_location": safe_float(row.get("pipLocation"), -2.0 if instrument.endswith("_JPY") else -4.0),
                "display_precision": max(0.0, safe_float(row.get("displayPrecision"), 5.0)),
                "minimum_trailing_stop_distance": max(
                    0.0,
                    safe_float(row.get("minimumTrailingStopDistance")),
                ),
            }
        trades = self.open_trades()
        owned = [trade for trade in trades if self.owns_trade(trade)]
        self.has_owned_trade = bool(owned)
        self.owned_trade_state = {
            str(trade.get("id") or ""): self._trade_reentry_identity(trade)
            for trade in owned
            if trade.get("id")
        }
        self._load_reentry_state()
        self._seed_reentry_from_recent_transactions(summary)
        # Re-evaluate at initialization because the dedicated launcher binds
        # its read-only feed after constructing the shared executor object.
        late_cache_recovery = self.ensure_async_signal_feed_cache()
        if self.signal_feed_cache is not None:
            self.signal_feed_cache.start()
        if self.signal_snapshot_writer is not None:
            self.signal_snapshot_writer.start()
        log_line(
            self.log_path,
            "practice_execution_ready",
            account_suffix=self.account_id[-4:],
            execution_enabled=bool(getattr(self.args, "execute_top_signals", False)),
            balance=self.start_balance,
            units=self.args.execution_units,
            sizing_mode="confidence_risk_margin",
            signal_selection_mode="individual_signal_stream",
            signal_feed_database="" if self.signal_feed is None else str(self.signal_feed.path.resolve()),
            min_signal_confidence=getattr(self.args, "execution_min_signal_confidence", 0.54),
            min_signal_expected_net_pips=getattr(self.args, "execution_min_signal_expected_net_pips", 0.05),
            max_open_positions=getattr(self.args, "execution_max_open_positions", 1),
            target_margin_used_pct=getattr(self.args, "execution_target_margin_used_pct", 55.0),
            hard_margin_used_pct=getattr(self.args, "execution_hard_margin_used_pct", 75.0),
            open_ended_profit=bool(getattr(self.args, "execution_open_ended_profit", False)),
            open_trades=len(trades),
            cooldown_sec=self.args.execution_cooldown_sec,
            pair_direction_reentry_cooldown_sec=safe_float(
                getattr(self.args, "execution_reentry_cooldown_sec", 900.0),
                900.0,
            ),
            instrument_reentry_cooldown_sec=safe_float(
                getattr(
                    self.args,
                    "execution_instrument_reentry_cooldown_sec",
                    300.0,
                ),
                300.0,
            ),
            jpy_factor_reentry_cooldown_sec=safe_float(
                getattr(
                    self.args,
                    "execution_jpy_factor_cooldown_sec",
                    900.0,
                ),
                900.0,
            ),
            max_hold_sec=self.args.execution_max_hold_sec,
            max_loss_account=self.args.execution_max_loss_account,
            intrahour_cost_capture_policy={
                "enabled": bool(
                    getattr(self.args, "execution_intrahour_cost_gate", True)
                ),
                "max_horizon_sec": int(
                    getattr(
                        self.args,
                        "execution_intrahour_cost_gate_max_horizon_sec",
                        3600,
                    )
                ),
                "max_spread_pips": safe_float(
                    getattr(
                        self.args,
                        "execution_intrahour_max_spread_pips",
                        3.0,
                    ),
                    3.0,
                ),
                "min_liquidity_quality": safe_float(
                    getattr(
                        self.args,
                        "execution_intrahour_min_liquidity_quality",
                        0.55,
                    ),
                    0.55,
                ),
                "min_confidence": safe_float(
                    getattr(
                        self.args,
                        "execution_intrahour_min_confidence",
                        0.55,
                    ),
                    0.55,
                ),
                "min_gross_to_spread": safe_float(
                    getattr(
                        self.args,
                        "execution_intrahour_min_gross_to_spread",
                        1.75,
                    ),
                    1.75,
                ),
                "min_after_cost_pips": safe_float(
                    getattr(
                        self.args,
                        "execution_intrahour_min_after_cost_pips",
                        0.25,
                    ),
                    0.25,
                ),
            },
            horizon_scaled_protection=bool(
                getattr(self.args, "execution_horizon_scaled_protection", True)
            ),
            ranking_horizon_sec=self.performance.horizon_sec,
            ranking_horizons_sec=list(self.args.execution_horizons),
            promotion_state="" if self.promotion is None else str(self.promotion.state_path.resolve()),
            min_samples=self.args.execution_min_samples,
            top_lane_count=self.args.execution_top_lanes,
            min_average_pips=self.args.execution_min_average_pips,
            min_median_pips=self.args.execution_min_median_pips,
            min_win_rate=self.args.execution_min_win_rate,
            min_lower_confidence_pips=self.args.execution_min_lower_confidence_pips,
            fitted_exits_enabled=bool(self.args.execution_use_fitted_exits),
            prediction_quality_enabled=bool(
                getattr(self.args, "execution_prediction_quality", True)
            ),
            prediction_quality_min_samples=int(
                safe_float(
                    getattr(self.args, "execution_prediction_quality_min_samples", 30),
                    30,
                )
            ),
            second_curve_entry_veto=bool(
                getattr(self.args, "execution_second_curve_entry_veto", True)
            ),
            second_curve_profit_exit=bool(
                getattr(self.args, "execution_second_curve_profit_exit", True)
            ),
            execution_policy=(
                {}
                if self.execution_policy is None
                else self.execution_policy.summary()
            ),
            async_signal_feed_cache=(
                {}
                if self.signal_feed_cache is None
                else {
                    "enabled": True,
                    "refresh_sec": self.signal_feed_cache.refresh_sec,
                    "ttl_sec": self.signal_feed_cache.ttl_sec,
                    "limit": self.signal_feed_cache.limit,
                    "stale_policy": "fail_closed_for_new_entries",
                }
            ),
            async_signal_feed_cache_recovered_at_initialize=(
                late_cache_recovery
            ),
            async_signal_snapshot={
                "enabled": self.signal_snapshot_writer is not None,
                "mode": "coalescing_background_writer",
            },
        )

    def close(self) -> None:
        if self.signal_snapshot_writer is not None:
            self.signal_snapshot_writer.stop()
            self.signal_snapshot_writer = None
        if self.signal_feed_cache is not None:
            self.signal_feed_cache.stop()
            self.signal_feed_cache = None
        if self.signal_feed is not None:
            self.signal_feed.close()
            self.signal_feed = None

    def open_trades(self) -> list[dict[str, Any]]:
        payload = self.client.get(f"/v3/accounts/{self.account_id}/openTrades", params={})
        return [trade for trade in payload.get("trades") or [] if isinstance(trade, dict)]

    @staticmethod
    def owns_trade(trade: dict[str, Any]) -> bool:
        extensions = trade.get("clientExtensions") or {}
        return str(extensions.get("tag") or "") == "strategy_lab_top"

    @staticmethod
    def directional_currency_keys(
        instrument: str,
        direction: str,
    ) -> set[tuple[str, int]]:
        pair = str(instrument or "").split("_")
        if len(pair) != 2:
            return set()
        sign = 1 if str(direction).lower() in {"buy", "long"} else -1
        return {(pair[0], sign), (pair[1], -sign)}

    @classmethod
    def jpy_factor_key(cls, instrument: str, direction: str) -> str:
        for currency, sign in cls.directional_currency_keys(
            instrument,
            direction,
        ):
            if currency == "JPY":
                return f"JPY:{'long' if sign > 0 else 'short'}"
        return ""

    def _trade_reentry_identity(self, trade: dict[str, Any]) -> dict[str, Any]:
        instrument = str(trade.get("instrument") or "")
        direction = "buy" if safe_float(trade.get("currentUnits")) > 0.0 else "sell"
        return {
            "instrument": instrument,
            "direction": direction,
            "factor": self.jpy_factor_key(instrument, direction),
            "open_time": str(trade.get("openTime") or ""),
        }

    def _load_reentry_state(self) -> None:
        try:
            payload = json.loads(self.reentry_state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        now_epoch = time.time()
        max_age = max(
            86400.0,
            safe_float(
                getattr(self.args, "execution_reentry_history_sec", 172800.0),
                172800.0,
            ),
        )
        for target, key in (
            (self.recent_pair_direction_exits, "pair_direction_exits"),
            (self.recent_instrument_exits, "instrument_exits"),
            (self.recent_factor_exits, "factor_exits"),
        ):
            values = payload.get(key) or {}
            if not isinstance(values, dict):
                continue
            for name, value in values.items():
                epoch = safe_float(value)
                if epoch > 0.0 and now_epoch - epoch <= max_age:
                    target[str(name)] = epoch

    def _persist_reentry_state(self) -> None:
        try:
            atomic_json(
                self.reentry_state_path,
                {
                    "schema_version": 1,
                    "updated_at": utc_now(),
                    "account_suffix": self.account_id[-4:],
                    "pair_direction_exits": self.recent_pair_direction_exits,
                    "instrument_exits": self.recent_instrument_exits,
                    "factor_exits": self.recent_factor_exits,
                },
            )
        except OSError as exc:
            log_line(
                self.log_path,
                "practice_reentry_state_error",
                error=f"{type(exc).__name__}: {exc}"[:300],
            )

    def _record_trade_exit(
        self,
        identity: dict[str, Any],
        *,
        exit_epoch: float | None = None,
        trade_id: str = "",
        source: str = "broker_observation",
    ) -> None:
        instrument = str(identity.get("instrument") or "")
        direction = str(identity.get("direction") or "")
        if not instrument or direction not in {"buy", "sell"}:
            return
        epoch = safe_float(exit_epoch, time.time())
        if epoch <= 0.0:
            epoch = time.time()
        pair_key = f"{instrument}|{direction}"
        factor = str(
            identity.get("factor")
            or self.jpy_factor_key(instrument, direction)
        )
        self.recent_pair_direction_exits[pair_key] = max(
            epoch,
            self.recent_pair_direction_exits.get(pair_key, 0.0),
        )
        self.recent_instrument_exits[instrument] = max(
            epoch,
            self.recent_instrument_exits.get(instrument, 0.0),
        )
        if factor:
            self.recent_factor_exits[factor] = max(
                epoch,
                self.recent_factor_exits.get(factor, 0.0),
            )
        self._persist_reentry_state()
        log_line(
            self.log_path,
            "practice_trade_exit_observed",
            trade_id=trade_id,
            instrument=instrument,
            direction=direction,
            correlation_factor=factor,
            source=source,
        )

    def _seed_reentry_from_recent_transactions(
        self,
        summary: dict[str, Any],
    ) -> None:
        """Recover recent owned exits so a process restart cannot reset cooldowns."""
        try:
            last_id = int(str(summary.get("lastTransactionID") or "0"))
        except ValueError:
            return
        if last_id <= 0:
            return
        try:
            payload = self.client.get(
                f"/v3/accounts/{self.account_id}/transactions/idrange",
                params={"from": max(1, last_id - 300), "to": last_id},
            )
        except OandaApiError as exc:
            log_line(
                self.log_path,
                "practice_reentry_seed_error",
                status=exc.status,
                error=str(exc)[:300],
            )
            return
        owned_order_ids: set[str] = set()
        owned_trade_ids: dict[str, dict[str, Any]] = {}
        seeded = 0
        transactions = sorted(
            [row for row in payload.get("transactions") or [] if isinstance(row, dict)],
            key=lambda row: int(safe_float(row.get("id"))),
        )
        for transaction in transactions:
            extensions = transaction.get("clientExtensions") or {}
            client_order_id = str(transaction.get("clientOrderID") or "")
            if (
                str(extensions.get("tag") or "") == "strategy_lab_top"
                or client_order_id.startswith("lab-")
            ):
                owned_order_ids.add(str(transaction.get("id") or ""))
            if str(transaction.get("type") or "") != "ORDER_FILL":
                continue
            order_id = str(transaction.get("orderID") or "")
            instrument = str(transaction.get("instrument") or "")
            opened = transaction.get("tradeOpened") or {}
            if opened and (
                order_id in owned_order_ids or client_order_id.startswith("lab-")
            ):
                units = safe_float(opened.get("units"), safe_float(transaction.get("units")))
                direction = "buy" if units > 0.0 else "sell"
                trade_id = str(opened.get("tradeID") or "")
                if trade_id:
                    owned_trade_ids[trade_id] = {
                        "instrument": instrument,
                        "direction": direction,
                        "factor": self.jpy_factor_key(instrument, direction),
                    }
            closed_rows = list(transaction.get("tradesClosed") or [])
            reduced = transaction.get("tradeReduced") or {}
            if reduced:
                closed_rows.append(reduced)
            for closed in closed_rows:
                trade_id = str(closed.get("tradeID") or "")
                identity = owned_trade_ids.pop(trade_id, None)
                if identity is None:
                    continue
                parsed = parse_rfc3339(str(transaction.get("time") or ""))
                self._record_trade_exit(
                    identity,
                    exit_epoch=None if parsed is None else parsed.timestamp(),
                    trade_id=trade_id,
                    source="recent_transaction_recovery",
                )
                seeded += 1
        if seeded:
            log_line(
                self.log_path,
                "practice_reentry_seeded",
                recent_exit_count=seeded,
                transaction_count=len(transactions),
            )

    def _observe_owned_trades(self, trades: list[dict[str, Any]]) -> None:
        current = {
            str(trade.get("id") or ""): self._trade_reentry_identity(trade)
            for trade in trades
            if self.owns_trade(trade) and trade.get("id")
        }
        for trade_id, identity in self.owned_trade_state.items():
            if trade_id and trade_id not in current:
                self._record_trade_exit(
                    identity,
                    trade_id=trade_id,
                    source="open_trade_reconciliation",
                )
        self.owned_trade_state = current
        self.has_owned_trade = bool(current)

    @staticmethod
    def _comment_number(comment: str, key: str) -> float:
        match = re.search(rf"(?:^|\|){re.escape(key)}(-?\d+(?:\.\d+)?)", comment)
        return safe_float(match.group(1)) if match else 0.0

    @staticmethod
    def add_signal_agreement(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        votes: dict[str, dict[str, set[str]]] = defaultdict(lambda: {"buy": set(), "sell": set()})
        vote_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for candidate in candidates:
            instrument = str(candidate.get("instrument") or "")
            direction = str(candidate.get("direction") or "")
            family = str(candidate.get("family") or candidate.get("lane_id") or "")
            if (
                str(candidate.get("signal_role") or "") == "entry_exit_timing"
                and candidate.get("projected_net_pips") is not None
                and safe_float(candidate.get("projected_net_pips")) < 0.0
            ):
                continue
            if instrument and direction in {"buy", "sell"} and family:
                votes[instrument][direction].add(family)
                vote_rows[instrument].append(candidate)
        archetype_summaries: dict[tuple[str, str], dict[str, Any]] = {}
        output: list[dict[str, Any]] = []
        for candidate in candidates:
            row = dict(candidate)
            instrument = str(row.get("instrument") or "")
            direction = str(row.get("direction") or "")
            opposite = "sell" if direction == "buy" else "buy"
            agreeing = len(votes[instrument][direction])
            opposing = len(votes[instrument][opposite])
            row["agreement_family_count"] = agreeing
            row["opposing_family_count"] = opposing
            row["agreement_probability"] = round((agreeing + 1.0) / (agreeing + opposing + 2.0), 6)
            row["strategy_archetype"] = strategy_archetype(
                row.get("family") or row.get("lane_id")
            )
            cache_key = (instrument, direction)
            summary = archetype_summaries.get(cache_key)
            if summary is None:
                summary = archetype_vote_summary(
                    vote_rows.get(instrument, []),
                    target_direction=direction,
                )
                archetype_summaries[cache_key] = summary
            row["agreement_archetype_count"] = int(
                summary["agreeing_archetype_count"]
            )
            row["opposing_archetype_count"] = int(
                summary["opposing_archetype_count"]
            )
            row["conflicted_archetype_count"] = int(
                summary["conflicted_archetype_count"]
            )
            row["agreement_archetype_probability"] = summary[
                "agreement_probability"
            ]
            row["agreement_archetypes"] = list(summary["agreeing_archetypes"])
            row["opposing_archetypes"] = list(summary["opposing_archetypes"])
            row["conflicted_archetypes"] = list(summary["conflicted_archetypes"])
            row["strategy_independence_shadow_only"] = True
            output.append(row)
        return output

    def paper_consensus_policy(self) -> dict[str, Any]:
        return {
            "enabled": bool(
                getattr(self.args, "execution_paper_consensus", False)
            ),
            "account_scope": "practice_007_only",
            "min_families": int(
                safe_float(
                    getattr(
                        self.args,
                        "execution_paper_consensus_min_families",
                        3,
                    ),
                    3,
                )
            ),
            "min_aligned_weight_pct": safe_float(
                getattr(
                    self.args,
                    "execution_paper_consensus_min_aligned_weight_pct",
                    65.0,
                ),
                65.0,
            ),
            "min_confidence": safe_float(
                getattr(
                    self.args,
                    "execution_paper_consensus_min_confidence",
                    0.545,
                ),
                0.545,
            ),
            "min_net_pips": safe_float(
                getattr(
                    self.args,
                    "execution_paper_consensus_min_net_pips",
                    0.15,
                ),
                0.15,
            ),
            "min_gross_to_spread": safe_float(
                getattr(
                    self.args,
                    "execution_paper_consensus_min_gross_to_spread",
                    1.15,
                ),
                1.15,
            ),
            "max_horizon_sec": int(
                safe_float(
                    getattr(
                        self.args,
                        "execution_paper_consensus_max_horizon_sec",
                        3600,
                    ),
                    3600,
                )
            ),
        }

    @staticmethod
    def _liquidity_prior(instrument: str) -> float:
        parts = str(instrument or "").upper().split("_")
        if len(parts) != 2:
            return 0.35
        base, quote = parts
        g10 = {"USD", "EUR", "JPY", "GBP", "AUD", "CAD", "CHF", "NZD"}
        regional = {"SGD", "HKD", "NOK", "SEK", "DKK"}
        if "USD" in parts and base in g10 and quote in g10:
            return 1.0
        if base in g10 and quote in g10:
            return 0.85
        if base in g10 and quote in regional or quote in g10 and base in regional:
            return 0.60
        if base in regional and quote in regional:
            return 0.48
        return 0.28

    def enrich_candidate_economics(
        self,
        candidates: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for candidate in candidates:
            row = dict(candidate)
            instrument = str(row.get("instrument") or "")
            meta = self.instrument_meta.get(instrument) or {}
            margin_rate = max(1e-6, safe_float(meta.get("margin_rate"), 0.02))
            bid = safe_float(row.get("bid"))
            ask = safe_float(row.get("ask"))
            mid = (bid + ask) / 2.0 if bid > 0.0 and ask > 0.0 else max(bid, ask)
            pip = max(1e-12, safe_float(row.get("pip")))
            prior = self._liquidity_prior(instrument)
            bid_depth = max(0.0, safe_float(row.get("bid_total_liquidity")))
            ask_depth = max(0.0, safe_float(row.get("ask_total_liquidity")))
            balanced_depth = min(bid_depth, ask_depth)
            if balanced_depth > 0.0:
                depth_quality = min(1.0, math.sqrt(balanced_depth / 5_000_000.0))
                liquidity_quality = 0.75 * depth_quality + 0.25 * prior
            else:
                depth_quality = 0.0
                liquidity_quality = prior
            imbalance = abs(safe_float(row.get("depth_imbalance")))
            liquidity_quality *= max(0.70, 1.0 - 0.30 * min(1.0, imbalance))
            row.update(
                {
                    "margin_rate": margin_rate,
                    "liquidity_prior": round(prior, 6),
                    "depth_quality": round(depth_quality, 6),
                    "liquidity_quality": round(
                        max(0.10, min(1.0, liquidity_quality)),
                        6,
                    ),
                    "pip_return_on_margin_per_pip_pct": round(
                        100.0 * pip / max(1e-12, mid * margin_rate),
                        8,
                    )
                    if mid > 0.0
                    else 0.0,
                }
            )
            output.append(row)
        return output

    @staticmethod
    def _signal_rank_key(row: dict[str, Any]) -> tuple[Any, ...]:
        return (
            bool(row.get("signal_eligible")),
            safe_float(row.get("normalized_rank_score"), -999.0),
            safe_float(row.get("signal_score"), -999.0),
            safe_float(row.get("signal_confidence")),
            safe_float(row.get("instant_projected_net_pips"), -999.0),
            -safe_float(row.get("spread_pips")),
        )

    @staticmethod
    def candidate_validation_state(
        row: dict[str, Any],
        recent_evidence: dict[str, Any],
        prediction_quality: dict[str, Any] | None = None,
        *,
        min_historical_reliability: float = 0.15,
    ) -> dict[str, Any]:
        promotion = row.get("promotion_evidence") or {}
        raw = promotion.get("raw") or {}
        raw_n = int(safe_float(raw.get("n")))
        raw_avg = safe_float(raw.get("avg"))
        raw_win_rate = safe_float(raw.get("win_rate"))
        representative_negative_historical_warmup = bool(
            (
                raw_n >= 5
                and raw_avg < 0.0
                and raw_win_rate <= 40.0
            )
            or (
                raw_n >= 30
                and raw_avg < 0.0
                and raw_win_rate < 50.0
            )
        )
        paper_consensus = row.get("paper_consensus_evidence") or {}
        paper_consensus_ready = bool(
            isinstance(paper_consensus, dict)
            and paper_consensus.get("eligible")
            and paper_consensus.get("account_scope") == "practice_007_only"
        )
        # Consensus breadth is research evidence only.  It must never erase
        # negative history or substitute for a governed, promoted candidate.
        negative_historical_warmup = representative_negative_historical_warmup
        holdout = promotion.get("holdout") or {}
        promotion_negative = bool(
            promotion
            and promotion.get("eligible") is False
            and (
                representative_negative_historical_warmup
                or (
                    int(safe_float(holdout.get("n"))) >= 4
                    and safe_float(holdout.get("avg")) <= 0.0
                    and safe_float(holdout.get("win_rate")) < 50.0
                )
            )
        )
        promotion_rejected = promotion_negative
        promotion_incomplete = bool(
            promotion
            and promotion.get("eligible") is False
            and not promotion_negative
        )
        quality = prediction_quality or {}
        quality_ready = bool(quality.get("eligible") and not promotion_rejected)
        quality_negative = bool(quality.get("negative_evidence"))
        historical_ready = bool(
            not promotion_rejected
            and
            safe_float(row.get("historical_reliability"))
            >= max(0.0, float(min_historical_reliability))
            and safe_float(row.get("sample_adjusted_historical_edge_pips")) > 0.0
            and safe_float(row.get("historical_win_probability")) >= 0.5
        )
        recent_ready = bool(recent_evidence.get("positive_ready"))
        return {
            "validated": bool(
                (
                    historical_ready
                    or recent_ready
                    or quality_ready
                )
                and not negative_historical_warmup
                and not quality_negative
            ),
            "historical_ready": historical_ready,
            "recent_ready": recent_ready,
            "prediction_quality_ready": quality_ready,
            "prediction_quality_negative": quality_negative,
            "negative_historical_warmup": negative_historical_warmup,
            "representative_negative_historical_warmup": (
                representative_negative_historical_warmup
            ),
            "promotion_rejected": promotion_rejected,
            "promotion_incomplete": promotion_incomplete,
            "paper_consensus_ready": paper_consensus_ready,
            "paper_consensus_shadow_only": True,
            "minimum_historical_reliability": float(min_historical_reliability),
        }

    def apply_prediction_quality(self, row: dict[str, Any]) -> dict[str, Any]:
        if (
            not bool(getattr(self.args, "execution_prediction_quality", True))
            or self.exit_fit is None
            or not hasattr(self.exit_fit, "quality_evidence")
        ):
            return {}
        horizon = int(safe_float(row.get("execution_horizon_sec")))
        if horizon <= 0:
            return {}
        regime = str(
            row.get("volatility_regime")
            or classify_volatility_regime(
                row.get("atr_pips"),
                row.get("spread_pips"),
            )
        )
        minimum_samples = int(
            safe_float(
                getattr(self.args, "execution_prediction_quality_min_samples", 30),
                30,
            )
        )
        evidence = self.exit_fit.quality_evidence(
            str(row.get("lane_id") or ""),
            str(row.get("family") or ""),
            str(row.get("instrument") or ""),
            horizon,
            regime,
            model_id=str(row.get("model_id") or ""),
            input_timeframe=str(row.get("input_timeframe") or ""),
            direction=str(row.get("direction") or ""),
            entry_time=str(row.get("entry_time") or ""),
            min_samples=minimum_samples,
        )
        if not evidence:
            return {}
        raw_confidence = safe_float(row.get("signal_confidence"), 0.5)
        raw_edge = safe_float(row.get("projected_net_pips"))
        adjusted_confidence = raw_confidence
        adjusted_edge = raw_edge
        weight = 0.0
        if evidence.get("eligible"):
            maximum_weight = max(
                0.0,
                min(
                    1.0,
                    safe_float(
                        getattr(
                            self.args,
                            "execution_prediction_quality_max_weight",
                            0.75,
                        ),
                        0.75,
                    ),
                ),
            )
            weight = min(
                maximum_weight,
                max(0.0, safe_float(evidence.get("evidence_strength"))),
            )
            adjusted_confidence = (
                (1.0 - weight) * raw_confidence
                + weight
                * safe_float(
                    evidence.get("calibrated_positive_probability"),
                    raw_confidence,
                )
            )
            adjusted_edge = (
                (1.0 - weight) * raw_edge
                + weight
                * safe_float(evidence.get("calibrated_expected_net_pips"), raw_edge)
            )
            row["uncalibrated_signal_confidence"] = raw_confidence
            row["uncalibrated_projected_net_pips"] = raw_edge
            row["signal_confidence"] = round(adjusted_confidence, 6)
            row["projected_net_pips"] = round(adjusted_edge, 4)
        blockers = list(row.get("signal_blocked_by") or [])
        if (
            bool(
                getattr(
                    self.args,
                    "execution_prediction_quality_negative_veto",
                    True,
                )
            )
            and evidence.get("negative_evidence")
        ):
            blockers.append("prediction_quality_negative")
        if evidence.get("eligible"):
            if adjusted_confidence < safe_float(
                getattr(self.args, "execution_min_signal_confidence", 0.54),
                0.54,
            ):
                blockers.append("prediction_quality_confidence")
            if adjusted_edge < safe_float(
                getattr(self.args, "execution_min_signal_expected_net_pips", 0.05),
                0.05,
            ):
                blockers.append("prediction_quality_net_edge")
            timing = (evidence.get("holdout") or {}).get("median_time_to_positive_sec")
            if timing is not None and safe_float(timing) > horizon:
                blockers.append("prediction_quality_timing")
        if blockers:
            row["signal_blocked_by"] = sorted(set(blockers))
            if any(blocker.startswith("prediction_quality_") for blocker in blockers):
                row["signal_eligible"] = False
        return {
            **evidence,
            "raw_signal_confidence": round(raw_confidence, 6),
            "adjusted_signal_confidence": round(adjusted_confidence, 6),
            "raw_projected_net_pips": round(raw_edge, 4),
            "adjusted_projected_net_pips": round(adjusted_edge, 4),
            "calibration_weight": round(weight, 6),
        }

    def apply_prediction_quality_curve(self, row: dict[str, Any]) -> None:
        """Calibrate every horizon point before cross-model consensus weighting."""

        curve = row.get("signal_horizon_curve") or []
        if not isinstance(curve, list) or not curve:
            row["prediction_quality"] = self.apply_prediction_quality(row)
            return
        selected_horizon = int(safe_float(row.get("execution_horizon_sec")))
        selected_update: dict[str, Any] | None = None
        selected_evidence: dict[str, Any] = {}
        for point in curve:
            if not isinstance(point, dict):
                continue
            synthetic = {
                **row,
                **point,
                "execution_horizon_sec": int(safe_float(point.get("horizon_sec"))),
                "signal_blocked_by": list(point.get("signal_blocked_by") or []),
            }
            evidence = self.apply_prediction_quality(synthetic)
            point["prediction_quality"] = evidence
            for key in (
                "signal_confidence",
                "projected_net_pips",
                "signal_eligible",
                "signal_blocked_by",
                "uncalibrated_signal_confidence",
                "uncalibrated_projected_net_pips",
            ):
                if key in synthetic:
                    point[key] = synthetic[key]
            if int(safe_float(point.get("horizon_sec"))) == selected_horizon:
                selected_update = synthetic
                selected_evidence = evidence
        if selected_update is not None:
            for key in (
                "signal_confidence",
                "projected_net_pips",
                "signal_eligible",
                "signal_blocked_by",
                "uncalibrated_signal_confidence",
                "uncalibrated_projected_net_pips",
            ):
                if key in selected_update:
                    row[key] = selected_update[key]
            row["prediction_quality"] = selected_evidence

    @staticmethod
    def _timeframe_seconds(value: str) -> int:
        match = re.fullmatch(r"([SMHD])(\d+)", str(value or "").upper())
        if not match:
            return 0
        unit_seconds = {"S": 1, "M": 60, "H": 3600, "D": 86400}
        return unit_seconds[match.group(1)] * int(match.group(2))

    @staticmethod
    def _raw_matrix_component_weight(
        row: dict[str, Any],
        correlated_count: int,
    ) -> float:
        """Give every finite forecast one family-capped research vote."""

        input_weight = max(
            0.01,
            min(1.0, safe_float(row.get("matrix_input_weight"), 1.0)),
        )
        return input_weight / max(1, int(correlated_count))

    @staticmethod
    def _matrix_component_weight(row: dict[str, Any], correlated_count: int) -> float:
        """Attenuate filters while capping correlated family influence."""

        # Research-only/account-ineligible forecasts belong to the raw audit
        # surface. Letting them leak into the filtered surface can turn a
        # one-sided shadow family into the apparent bot consensus even though
        # that family is explicitly barred from account decisions.
        if row.get("research_only") or not row.get("account_eligible", True):
            return 0.0
        reliability = max(0.0, min(1.0, safe_float(row.get("historical_reliability"))))
        evidence_weight = 0.35 + 0.65 * reliability
        eligibility_weight = 1.0 if row.get("signal_eligible") else 0.35
        role_weight = (
            0.35
            if str(row.get("signal_role") or "structural") == "entry_exit_timing"
            else 1.0
        )
        input_weight = max(
            0.05,
            min(1.0, safe_float(row.get("matrix_input_weight"), 1.0)),
        )
        quality = row.get("prediction_quality") or {}
        quality_weight = 1.0
        if isinstance(quality, dict) and quality:
            if quality.get("negative_evidence"):
                # A chronological negative-evidence veto is a rejection, not
                # a small-confidence vote. Preserve it in the raw surface for
                # diagnostics, but do not let a known losing cell steer the
                # filtered forecast.
                return 0.0
            elif quality.get("eligible"):
                quality_weight = 0.50 + 0.50 * max(
                    0.0,
                    min(1.0, safe_float(quality.get("evidence_strength"))),
                )
            elif int(safe_float(quality.get("trial_count"))) > 1:
                quality_weight = 0.35
        sma_weight = validated_filter_weight(
            row.get("sma_filter"),
            int(
                safe_float(
                    row.get("horizon_sec"),
                    safe_float(row.get("execution_horizon_sec"), 300),
                )
            ),
        )
        return (
            evidence_weight
            * eligibility_weight
            * role_weight
            * input_weight
            * quality_weight
            * sma_weight
            / max(1, int(correlated_count))
        )

    @staticmethod
    def _execution_matrix_component_weight(
        row: dict[str, Any],
        filtered_weight: float,
    ) -> float:
        if (
            not row.get("signal_eligible")
            or not row.get("account_eligible", True)
            or str(row.get("signal_role") or "structural")
            == "entry_exit_timing"
        ):
            return 0.0
        return max(0.0, filtered_weight)

    @classmethod
    def _aggregate_horizon_contributors(
        cls,
        horizon: int,
        contributors: list[dict[str, Any]],
    ) -> dict[str, Any]:
        correlated_counts = Counter(
            (
                str(row.get("family") or "unknown"),
                str(row.get("signal_role") or "structural"),
            )
            for row in contributors
        )
        weighted: list[dict[str, Any]] = []
        for source in contributors:
            row = dict(source)
            sma_point = filter_point_for_horizon(row.get("sma_filter"), horizon)
            if sma_point:
                row["sma_filter_point"] = sma_point
                row["sma_filter_weight"] = validated_filter_weight(
                    row.get("sma_filter"),
                    horizon,
                )
                if sma_point.get("account_eligible") and not sma_point.get("accepted"):
                    blocked = list(row.get("signal_blocked_by") or [])
                    if "sma_filter_reject" not in blocked:
                        blocked.append("sma_filter_reject")
                    row["signal_blocked_by"] = blocked
                    row["signal_eligible"] = False
            group_key = (
                str(row.get("family") or "unknown"),
                str(row.get("signal_role") or "structural"),
            )
            raw_weight = cls._raw_matrix_component_weight(
                row,
                correlated_counts[group_key],
            )
            weight = cls._matrix_component_weight(row, correlated_counts[group_key])
            execution_weight = cls._execution_matrix_component_weight(row, weight)
            direction = str(row.get("direction") or "")
            side_probability = max(
                0.001,
                min(0.999, safe_float(row.get("signal_confidence"), 0.5)),
            )
            probability_up = (
                side_probability if direction == "buy" else 1.0 - side_probability
            )
            side_sign = 1.0 if direction == "buy" else -1.0
            filter_reasons = [
                *[str(value) for value in row.get("preconsensus_blockers") or []],
                *[str(value) for value in row.get("signal_blocked_by") or []],
            ]
            if not row.get("account_eligible", True):
                filter_reasons.append(
                    str(row.get("research_blocked_reason") or "account_ineligible")
                )
            row.update(
                {
                    "strategy_archetype": strategy_archetype(
                        row.get("family") or row.get("lane_id")
                    ),
                    "raw_matrix_weight": round(raw_weight, 8),
                    "matrix_weight": round(weight, 8),
                    "execution_matrix_weight": round(execution_weight, 8),
                    "matrix_probability_up": round(probability_up, 6),
                    "matrix_signed_net_pips": round(
                        side_sign
                        * max(0.0, safe_float(row.get("projected_net_pips"))),
                        6,
                    ),
                    "matrix_filter_passed": bool(row.get("signal_eligible")),
                    "matrix_execution_passed": bool(execution_weight > 0.0),
                    "matrix_filter_reasons": sorted(set(filter_reasons)),
                }
            )
            weighted.append(row)

        raw_total_weight = sum(row["raw_matrix_weight"] for row in weighted)
        total_weight = sum(row["matrix_weight"] for row in weighted)
        execution_total_weight = sum(
            row["execution_matrix_weight"] for row in weighted
        )
        filtered_surface_available = total_weight > 1e-12
        raw_denominator = max(1e-12, raw_total_weight)
        denominator = max(1e-12, total_weight)
        raw_probability_up = sum(
            row["raw_matrix_weight"] * row["matrix_probability_up"]
            for row in weighted
        ) / raw_denominator
        probability_up = (
            sum(
                row["matrix_weight"] * row["matrix_probability_up"]
                for row in weighted
            )
            / denominator
            if filtered_surface_available
            else 0.5
        )
        execution_probability_up = (
            sum(
                row["execution_matrix_weight"] * row["matrix_probability_up"]
                for row in weighted
            )
            / execution_total_weight
            if execution_total_weight > 0.0
            else None
        )
        raw_signed_net_pips = sum(
            row["raw_matrix_weight"] * row["matrix_signed_net_pips"]
            for row in weighted
        ) / raw_denominator
        signed_net_pips = (
            sum(
                row["matrix_weight"] * row["matrix_signed_net_pips"]
                for row in weighted
            )
            / denominator
            if filtered_surface_available
            else 0.0
        )
        execution_signed_net_pips = (
            sum(
                row["execution_matrix_weight"] * row["matrix_signed_net_pips"]
                for row in weighted
            )
            / execution_total_weight
            if execution_total_weight > 0.0
            else None
        )
        consensus_probability_up = (
            execution_probability_up
            if execution_probability_up is not None
            else probability_up
        )
        consensus_signed_net_pips = (
            execution_signed_net_pips
            if execution_signed_net_pips is not None
            else signed_net_pips
        )
        movement_scale = (
            max(
                0.10,
                sum(
                    row["matrix_weight"]
                    * max(0.0, safe_float(row.get("projected_net_pips")))
                    for row in weighted
                )
                / denominator,
            )
            if filtered_surface_available
            else 0.10
        )
        direction_score = (
            0.70 * (2.0 * consensus_probability_up - 1.0)
            + 0.30 * math.tanh(consensus_signed_net_pips / movement_scale)
        )
        direction = "buy" if direction_score >= 0.0 else "sell"
        direction_state = direction if filtered_surface_available else "neutral"
        direction_sign = 1.0 if direction == "buy" else -1.0
        side_probability = (
            consensus_probability_up
            if direction == "buy"
            else 1.0 - consensus_probability_up
        )
        projected_net_pips = direction_sign * consensus_signed_net_pips

        def directional_weighted_average(field: str) -> float:
            return direction_sign * sum(
                row["matrix_weight"]
                * (1.0 if row.get("direction") == "buy" else -1.0)
                * max(0.0, safe_float(row.get(field)))
                for row in weighted
            ) / denominator

        def weighted_average(field: str) -> float:
            values = [
                (row["matrix_weight"], safe_float(row.get(field)))
                for row in weighted
                if row.get(field) is not None
            ]
            denominator = sum(weight for weight, _ in values)
            return (
                sum(weight * value for weight, value in values) / denominator
                if denominator > 0.0
                else 0.0
            )

        def aligned_weighted_average(field: str) -> float:
            """Average a magnitude using only contributors on the chosen side.

            The legacy all-contributor average remains published for continuity.
            Research gates must not treat an opposing contributor's projected
            cost coverage as evidence for the selected direction, however.
            """

            values = [
                (row["matrix_weight"], safe_float(row.get(field)))
                for row in weighted
                if row.get("direction") == direction
                and row.get(field) is not None
            ]
            aligned_denominator = sum(weight for weight, _ in values)
            return (
                sum(weight * value for weight, value in values)
                / aligned_denominator
                if aligned_denominator > 0.0
                else 0.0
            )

        support = [
            row
            for row in weighted
            if row.get("direction") == direction
            and row.get("signal_eligible")
            and row.get("account_eligible", True)
            and str(row.get("signal_role") or "structural") != "entry_exit_timing"
        ]
        account_structural = [
            row
            for row in weighted
            if row.get("direction") == direction
            and row.get("account_eligible", True)
            and str(row.get("signal_role") or "structural") != "entry_exit_timing"
        ]
        minimum_confidence = (
            sum(
                row["matrix_weight"]
                * safe_float(row.get("effective_min_signal_confidence"), 0.54)
                for row in account_structural
            )
            / max(
                1e-12,
                sum(row["matrix_weight"] for row in account_structural),
            )
            if account_structural
            else 0.54
        )
        minimum_edge = (
            sum(
                row["matrix_weight"]
                * safe_float(row.get("effective_min_expected_net_pips"), 0.05)
                for row in account_structural
            )
            / max(
                1e-12,
                sum(row["matrix_weight"] for row in account_structural),
            )
            if account_structural
            else 0.05
        )
        paper_policy = next(
            (
                row.get("paper_consensus_policy")
                for row in weighted
                if isinstance(row.get("paper_consensus_policy"), dict)
                and row.get("paper_consensus_policy", {}).get("enabled")
            ),
            {},
        )
        paper_soft_blockers = {
            "signal_confidence",
            "expected_net_edge",
            "current_cost_edge",
            "forecast_below_spread_buffer",
            "cold_start_evidence",
            "predictor_cell_not_promoted",
            "model_production_policy",
        }
        paper_candidates: list[dict[str, Any]] = []
        if (
            paper_policy.get("enabled")
            and paper_policy.get("account_scope") == "practice_007_only"
            and int(horizon)
            <= int(safe_float(paper_policy.get("max_horizon_sec"), 3600))
        ):
            for row in weighted:
                if (
                    not row.get("account_eligible", True)
                    or str(row.get("signal_role") or "structural")
                    != "structural"
                    or str(row.get("preconsensus_class") or "accepted")
                    != "accepted"
                ):
                    continue
                quality = row.get("prediction_quality") or {}
                if isinstance(quality, dict) and quality.get("negative_evidence"):
                    continue
                blockers_for_row = {
                    str(value) for value in row.get("signal_blocked_by") or []
                }
                if blockers_for_row.difference(paper_soft_blockers):
                    continue
                paper_candidates.append(row)

        paper_total_weight = sum(
            safe_float(row.get("matrix_weight")) for row in paper_candidates
        )
        paper_aligned = [
            row for row in paper_candidates if row.get("direction") == direction
        ]
        paper_aligned_weight = sum(
            safe_float(row.get("matrix_weight")) for row in paper_aligned
        )
        paper_aligned_weight_pct = (
            100.0 * paper_aligned_weight / paper_total_weight
            if paper_total_weight > 0.0
            else 0.0
        )
        paper_family_count = len(
            {str(row.get("family") or "") for row in paper_aligned}
        )
        paper_independence = archetype_vote_summary(
            paper_candidates,
            target_direction=direction,
        )
        paper_archetype_count = int(
            paper_independence["agreeing_archetype_count"]
        )
        paper_probability_up = (
            sum(
                safe_float(row.get("matrix_weight"))
                * safe_float(row.get("matrix_probability_up"), 0.5)
                for row in paper_candidates
            )
            / paper_total_weight
            if paper_total_weight > 0.0
            else 0.5
        )
        paper_side_probability = (
            paper_probability_up if direction == "buy" else 1.0 - paper_probability_up
        )
        paper_signed_net = (
            sum(
                safe_float(row.get("matrix_weight"))
                * (1.0 if row.get("direction") == "buy" else -1.0)
                * max(0.0, safe_float(row.get("projected_net_pips")))
                for row in paper_candidates
            )
            / paper_total_weight
            if paper_total_weight > 0.0
            else 0.0
        )
        paper_projected_net = direction_sign * paper_signed_net
        paper_gross_to_spread = (
            sum(
                safe_float(row.get("matrix_weight"))
                * safe_float(row.get("gross_to_spread"))
                for row in paper_aligned
                if row.get("gross_to_spread") is not None
            )
            / max(
                1e-12,
                sum(
                    safe_float(row.get("matrix_weight"))
                    for row in paper_aligned
                    if row.get("gross_to_spread") is not None
                ),
            )
            if paper_aligned
            else 0.0
        )
        paper_consensus_eligible = bool(
            paper_candidates
            and paper_family_count
            >= int(safe_float(paper_policy.get("min_families"), 3))
            and paper_aligned_weight_pct
            >= safe_float(paper_policy.get("min_aligned_weight_pct"), 65.0)
            and paper_side_probability
            >= max(
                minimum_confidence,
                safe_float(paper_policy.get("min_confidence"), 0.545),
            )
            and paper_projected_net
            >= max(
                minimum_edge,
                safe_float(paper_policy.get("min_net_pips"), 0.15),
            )
            and paper_gross_to_spread
            >= safe_float(paper_policy.get("min_gross_to_spread"), 1.15)
        )
        paper_consensus_evidence = {
            "eligible": paper_consensus_eligible,
            "independence_shadow_only": True,
            "account_scope": "practice_007_only",
            "horizon_sec": int(horizon),
            "family_count": paper_family_count,
            "archetype_count": paper_archetype_count,
            "opposing_archetype_count": int(
                paper_independence["opposing_archetype_count"]
            ),
            "conflicted_archetype_count": int(
                paper_independence["conflicted_archetype_count"]
            ),
            "archetype_aligned_weight_pct": paper_independence[
                "aligned_weight_pct"
            ],
            "archetype_threshold_would_pass": bool(
                paper_archetype_count
                >= int(safe_float(paper_policy.get("min_families"), 3))
            ),
            "independence": paper_independence,
            "candidate_count": len(paper_candidates),
            "aligned_candidate_count": len(paper_aligned),
            "aligned_weight_pct": round(paper_aligned_weight_pct, 3),
            "signal_confidence": round(paper_side_probability, 6),
            "projected_net_pips": round(paper_projected_net, 4),
            "gross_to_spread": round(paper_gross_to_spread, 6),
            "policy": paper_policy,
        }
        if paper_consensus_eligible:
            side_probability = paper_side_probability
            consensus_probability_up = paper_probability_up
            projected_net_pips = paper_projected_net
        blockers: list[str] = []
        if not filtered_surface_available:
            blockers.append("ensemble_no_filtered_structural_support")
        if not account_structural:
            blockers.append("structural_signal_required")
        elif not support and not paper_consensus_eligible:
            blockers.append("ensemble_no_eligible_structural_support")
        if side_probability < minimum_confidence:
            blockers.append("ensemble_signal_confidence")
        if projected_net_pips < minimum_edge:
            blockers.append("ensemble_expected_net_edge")

        ranked_contributors = sorted(
            weighted,
            key=lambda row: (
                row.get("direction") == direction,
                bool(row.get("signal_eligible")),
                safe_float(row.get("matrix_weight")),
                cls._signal_rank_key(row),
            ),
            reverse=True,
        )
        best = ranked_contributors[0]
        aligned_weight = sum(
            row["matrix_weight"]
            for row in weighted
            if row.get("direction") == direction
        )
        raw_aligned_weight = sum(
            row["raw_matrix_weight"]
            for row in weighted
            if row.get("direction") == direction
        )
        execution_aligned_weight = sum(
            row["execution_matrix_weight"]
            for row in weighted
            if row.get("direction") == direction
        )
        setup_class_counts = Counter(
            str(row.get("preconsensus_class") or "accepted")
            for row in weighted
        )
        source_kind_counts = Counter(
            str(row.get("source_kind") or "strategy_signal")
            for row in weighted
        )
        independence = archetype_vote_summary(
            weighted,
            target_direction=direction,
        )
        quality_candidates = [
            row.get("prediction_quality")
            for row in weighted
            if row.get("direction") == direction
            and isinstance(row.get("prediction_quality"), dict)
            and row.get("prediction_quality")
        ]
        ensemble_quality = (
            max(
                quality_candidates,
                key=lambda evidence: (
                    bool(evidence.get("eligible")),
                    safe_float(evidence.get("deflated_sharpe_probability")),
                    safe_float(evidence.get("evidence_strength")),
                    int(safe_float(evidence.get("sample_count"))),
                ),
            )
            if quality_candidates
            else {}
        )
        return {
            "horizon_sec": int(horizon),
            "direction": direction,
            "direction_state": direction_state,
            "signal_eligible": not blockers,
            "signal_blocked_by": blockers,
            "signal_confidence": round(side_probability, 6),
            "probability_up": round(consensus_probability_up, 6),
            "filtered_probability_up": round(probability_up, 6),
            "raw_probability_up": round(raw_probability_up, 6),
            "execution_probability_up": (
                None
                if execution_probability_up is None
                else round(execution_probability_up, 6)
            ),
            "signal_score": round(directional_weighted_average("signal_score"), 6),
            "normalized_rank_score": round(
                directional_weighted_average("normalized_rank_score"),
                8,
            ),
            "expected_margin_return_per_hour_pct": round(
                directional_weighted_average("expected_margin_return_per_hour_pct"),
                8,
            ),
            "confidence_adjusted_margin_return_per_hour_pct": round(
                directional_weighted_average(
                    "confidence_adjusted_margin_return_per_hour_pct"
                ),
                8,
            ),
            "projected_net_pips": round(projected_net_pips, 4),
            "projected_net_pips_per_hour": round(
                projected_net_pips * 3600.0 / max(1, int(horizon)),
                4,
            ),
            # Preserve the established field so this diagnostics-only addition
            # cannot silently alter the practice executor.  Shadow trials can
            # now use the directionally truthful companion value.
            "gross_to_spread": round(weighted_average("gross_to_spread"), 6),
            "all_contributor_gross_to_spread": round(
                weighted_average("gross_to_spread"), 6
            ),
            "directional_gross_to_spread": round(
                aligned_weighted_average("gross_to_spread"), 6
            ),
            "minimum_gross_to_spread": round(
                max(
                    1.0,
                    weighted_average("minimum_gross_to_spread") or 1.15,
                ),
                6,
            ),
            "execution_uncertainty_cost_pips": round(
                weighted_average("execution_uncertainty_cost_pips"),
                4,
            ),
            "ensemble_signed_net_pips": round(signed_net_pips, 4),
            "raw_ensemble_signed_net_pips": round(raw_signed_net_pips, 4),
            "execution_ensemble_signed_net_pips": (
                None
                if execution_signed_net_pips is None
                else round(execution_signed_net_pips, 4)
            ),
            "ensemble_direction_score": round(direction_score, 6),
            "ensemble_weight": round(total_weight, 6),
            "raw_ensemble_weight": round(raw_total_weight, 6),
            "execution_ensemble_weight": round(execution_total_weight, 6),
            "ensemble_aligned_weight_pct": round(
                100.0 * aligned_weight / denominator,
                3,
            ),
            "raw_ensemble_aligned_weight_pct": round(
                100.0 * raw_aligned_weight / raw_denominator,
                3,
            ),
            "execution_ensemble_aligned_weight_pct": (
                None
                if execution_total_weight <= 0.0
                else round(
                    100.0 * execution_aligned_weight / execution_total_weight,
                    3,
                )
            ),
            "effective_min_signal_confidence": round(minimum_confidence, 6),
            "effective_min_expected_net_pips": round(minimum_edge, 4),
            "best_family": best.get("family"),
            "best_model_id": best.get("model_id"),
            "best_input_timeframe": best.get("input_timeframe"),
            "best_steps_ahead": best.get("steps_ahead"),
            "component_count": len(weighted),
            "research_only_component_count": sum(
                bool(row.get("research_only"))
                or not row.get("account_eligible", True)
                for row in weighted
            ),
            "filtered_surface_available": filtered_surface_available,
            "filter_passed_component_count": sum(
                bool(row.get("matrix_filter_passed")) for row in weighted
            ),
            "execution_component_count": sum(
                bool(row.get("matrix_execution_passed")) for row in weighted
            ),
            "paper_consensus_component_count": len(paper_aligned),
            "paper_consensus_eligible": paper_consensus_eligible,
            "paper_consensus_evidence": paper_consensus_evidence,
            "eligible_component_count": len(support),
            "family_count": len({str(row.get("family") or "") for row in weighted}),
            "archetype_count": int(independence["archetype_count"]),
            "agreement_archetype_count": int(
                independence["agreeing_archetype_count"]
            ),
            "opposing_archetype_count": int(
                independence["opposing_archetype_count"]
            ),
            "conflicted_archetype_count": int(
                independence["conflicted_archetype_count"]
            ),
            "archetype_independence_ratio": independence["independence_ratio"],
            "archetype_aligned_weight_pct": independence["aligned_weight_pct"],
            "strategy_independence_shadow_only": True,
            "timeframe_count": len(
                {str(row.get("input_timeframe") or "") for row in weighted}
            ),
            "families": sorted({str(row.get("family") or "") for row in weighted}),
            "archetypes": sorted(
                {
                    str(row.get("strategy_archetype") or "unclassified")
                    for row in weighted
                }
            ),
            "strategy_independence": independence,
            "input_timeframes": sorted(
                {str(row.get("input_timeframe") or "") for row in weighted},
                key=cls._timeframe_seconds,
            ),
            "contributors": ranked_contributors,
            "setup_class_counts": dict(sorted(setup_class_counts.items())),
            "source_kind_counts": dict(sorted(source_kind_counts.items())),
            "model_gap_component_count": source_kind_counts.get(
                "live_model_forecast", 0
            ),
            "prediction_quality": ensemble_quality,
            "sma_filter": best.get("sma_filter") or {},
            "sma_filter_point": best.get("sma_filter_point") or {},
            "sma_filter_weight": round(
                weighted_average("sma_filter_weight") or 1.0,
                6,
            ),
            "aggregation_method": "family_capped_reliability_weighted_consensus",
            "aggregation_version": "all_signal_raw_filtered_execution_v5",
        }

    @classmethod
    def consolidate_ranked_signals(cls, ranked: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Return one all-signal consensus per instrument and outcome horizon."""

        deduplicated: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
        for candidate in ranked:
            instrument = str(candidate.get("instrument") or "")
            direction = str(candidate.get("direction") or "")
            family = str(candidate.get("family") or candidate.get("lane_id") or "unknown")
            role = str(candidate.get("signal_role") or "structural")
            source = str(
                candidate.get("model_id")
                or candidate.get("lane_id")
                or family
            )
            timeframe = str(candidate.get("input_timeframe") or "multi")
            key = (instrument, direction, source, timeframe, role)
            current = deduplicated.get(key)
            if instrument and direction in {"buy", "sell"} and (
                current is None or cls._signal_rank_key(candidate) > cls._signal_rank_key(current)
            ):
                deduplicated[key] = candidate

        instrument_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for candidate in deduplicated.values():
            instrument_groups[str(candidate["instrument"])].append(candidate)

        consolidated: list[dict[str, Any]] = []
        for instrument, components in instrument_groups.items():
            horizon_contributors: dict[int, list[dict[str, Any]]] = defaultdict(list)
            for component in components:
                curve = component.get("signal_horizon_curve") or [
                    {
                        "horizon_sec": int(safe_float(component.get("execution_horizon_sec"))),
                        "signal_eligible": bool(component.get("signal_eligible")),
                        "signal_blocked_by": list(component.get("signal_blocked_by") or []),
                        "signal_confidence": safe_float(component.get("signal_confidence")),
                        "signal_score": safe_float(component.get("signal_score")),
                        "normalized_rank_score": safe_float(
                            component.get("normalized_rank_score")
                        ),
                        "projected_net_pips": safe_float(component.get("projected_net_pips")),
                        "projected_net_pips_per_hour": safe_float(
                            component.get("projected_net_pips_per_hour")
                        ),
                        "unpenalized_projected_net_pips": safe_float(
                            component.get("unpenalized_projected_net_pips")
                        ),
                        "short_horizon_cost_pips": safe_float(
                            component.get("short_horizon_cost_pips")
                        ),
                        "execution_uncertainty_cost_pips": safe_float(
                            component.get("execution_uncertainty_cost_pips")
                        ),
                        "gross_to_spread": component.get("gross_to_spread"),
                        "minimum_gross_to_spread": safe_float(
                            component.get("minimum_gross_to_spread"),
                            1.15,
                        ),
                        "required_net_edge_pips": safe_float(
                            component.get("required_net_edge_pips")
                        ),
                        "historical_lower_bound_pips": safe_float(
                            component.get("historical_lower_bound_pips")
                        ),
                        "sample_adjusted_historical_edge_pips": safe_float(
                            component.get("sample_adjusted_historical_edge_pips")
                        ),
                        "historical_reliability": safe_float(component.get("historical_reliability")),
                    }
                ]
                family = str(component.get("family") or component.get("lane_id") or "unknown")
                model_id = str(component.get("model_id") or component.get("lane_id") or family)
                timeframe = str(component.get("input_timeframe") or "multi")
                timeframe_sec = cls._timeframe_seconds(timeframe)
                for point in curve:
                    horizon = int(safe_float(point.get("horizon_sec")))
                    if horizon <= 0:
                        continue
                    point_direction = str(
                        point.get("direction") or component.get("direction") or ""
                    )
                    if point_direction not in {"buy", "sell"}:
                        continue
                    horizon_contributors[horizon].append(
                        {
                            "forecast_id": str(component.get("id") or ""),
                            "family": family,
                            "model_id": model_id,
                            "lane_id": component.get("lane_id"),
                            "input_timeframe": timeframe,
                            "direction": point_direction,
                            "steps_ahead": (
                                round(horizon / timeframe_sec, 3) if timeframe_sec > 0 else None
                            ),
                            "signal_role": str(component.get("signal_role") or "structural"),
                            "account_eligible": bool(
                                point.get(
                                    "account_eligible",
                                    component.get("account_eligible", True),
                                )
                            ),
                            "research_only": bool(
                                point.get("research_only", component.get("research_only"))
                            ),
                            "research_blocked_reason": str(
                                point.get(
                                    "research_blocked_reason",
                                    component.get("research_blocked_reason", ""),
                                )
                                or ""
                            ),
                            "source_kind": str(
                                point.get(
                                    "source_kind",
                                    component.get("source_kind", "strategy_signal"),
                                )
                            ),
                            "feed_source": str(component.get("feed_source") or ""),
                            "paper_consensus_policy": (
                                component.get("paper_consensus_policy")
                                if isinstance(
                                    component.get("paper_consensus_policy"),
                                    dict,
                                )
                                else {}
                            ),
                            "signal_provenance": (
                                component.get("signal_provenance")
                                if isinstance(component.get("signal_provenance"), dict)
                                else {}
                            ),
                            "forecast_generated_epoch": component.get(
                                "forecast_generated_epoch"
                            ),
                            "signal_candle_time": component.get(
                                "signal_candle_time"
                            ),
                            "entry_time": component.get("entry_time"),
                            "forecast_age_sec_at_publish": component.get(
                                "forecast_age_sec_at_publish"
                            ),
                            "preconsensus_class": str(
                                point.get(
                                    "preconsensus_class",
                                    component.get("preconsensus_class", "accepted"),
                                )
                            ),
                            "preconsensus_blockers": list(
                                point.get(
                                    "preconsensus_blockers",
                                    component.get("preconsensus_blockers", []),
                                )
                                or []
                            ),
                            "matrix_input_weight": safe_float(
                                point.get(
                                    "matrix_input_weight",
                                    component.get("matrix_input_weight", 1.0),
                                ),
                                1.0,
                            ),
                            "signal_eligible": bool(point.get("signal_eligible")),
                            "signal_blocked_by": list(point.get("signal_blocked_by") or []),
                            "signal_confidence": safe_float(point.get("signal_confidence")),
                            "signal_score": safe_float(point.get("signal_score"), -999.0),
                            "normalized_rank_score": safe_float(
                                point.get("normalized_rank_score"),
                                -999.0,
                            ),
                            "expected_margin_return_per_hour_pct": safe_float(
                                point.get("expected_margin_return_per_hour_pct")
                            ),
                            "confidence_adjusted_margin_return_per_hour_pct": safe_float(
                                point.get(
                                    "confidence_adjusted_margin_return_per_hour_pct"
                                )
                            ),
                            "liquidity_quality": safe_float(
                                point.get("liquidity_quality"),
                                safe_float(component.get("liquidity_quality"), 0.5),
                            ),
                            "projected_net_pips": safe_float(point.get("projected_net_pips")),
                            "spread_pips": safe_float(
                                point.get(
                                    "spread_pips",
                                    component.get("spread_pips"),
                                )
                            ),
                            "signal_strength_pips": safe_float(
                                point.get(
                                    "signal_strength_pips",
                                    component.get("signal_strength_pips"),
                                )
                            ),
                            "projected_net_pips_per_hour": safe_float(
                                point.get("projected_net_pips_per_hour")
                            ),
                            "unpenalized_projected_net_pips": safe_float(
                                point.get("unpenalized_projected_net_pips")
                            ),
                            "short_horizon_cost_pips": safe_float(
                                point.get("short_horizon_cost_pips")
                            ),
                            "execution_uncertainty_cost_pips": safe_float(
                                point.get("execution_uncertainty_cost_pips")
                            ),
                            "gross_to_spread": point.get("gross_to_spread"),
                            "minimum_gross_to_spread": safe_float(
                                point.get("minimum_gross_to_spread"),
                                1.15,
                            ),
                            "required_net_edge_pips": safe_float(
                                point.get("required_net_edge_pips")
                            ),
                            "historical_lower_bound_pips": safe_float(
                                point.get("historical_lower_bound_pips")
                            ),
                            "sample_adjusted_historical_edge_pips": safe_float(
                                point.get("sample_adjusted_historical_edge_pips")
                            ),
                            "historical_reliability": safe_float(point.get("historical_reliability")),
                            "prediction_quality": (
                                point.get("prediction_quality")
                                if isinstance(point.get("prediction_quality"), dict)
                                else (
                                    component.get("prediction_quality")
                                    if isinstance(
                                        component.get("prediction_quality"),
                                        dict,
                                    )
                                    else {}
                                )
                            ),
                            "effective_min_signal_confidence": safe_float(
                                point.get(
                                    "effective_min_signal_confidence",
                                    component.get("effective_min_signal_confidence", 0.54),
                                ),
                                0.54,
                            ),
                            "effective_min_expected_net_pips": safe_float(
                                point.get(
                                    "effective_min_expected_net_pips",
                                    component.get("effective_min_expected_net_pips", 0.05),
                                ),
                                0.05,
                            ),
                        }
                    )

            horizon_breakdown = [
                cls._aggregate_horizon_contributors(horizon, contributors)
                for horizon, contributors in sorted(horizon_contributors.items())
                if contributors
            ]
            if not horizon_breakdown:
                continue
            for point in horizon_breakdown:
                point_direction = str(point.get("direction") or "")
                opposing = [
                    row
                    for row in point.get("contributors") or []
                    if str(row.get("direction") or "") != point_direction
                ]
                compact_opposing = [
                    {
                        "forecast_id": str(row.get("forecast_id") or ""),
                        "family": str(row.get("family") or ""),
                        "model_id": str(row.get("model_id") or ""),
                        "lane_id": str(row.get("lane_id") or ""),
                        "input_timeframe": str(row.get("input_timeframe") or ""),
                        "direction": str(row.get("direction") or ""),
                        "strategy_archetype": str(
                            row.get("strategy_archetype") or "unclassified"
                        ),
                        "signal_role": str(row.get("signal_role") or "structural"),
                        "account_eligible": bool(row.get("account_eligible", True)),
                        "research_only": bool(row.get("research_only")),
                        "signal_eligible": bool(row.get("signal_eligible")),
                        "matrix_execution_passed": bool(
                            row.get("matrix_execution_passed")
                        ),
                        "matrix_weight": safe_float(row.get("matrix_weight")),
                        "execution_matrix_weight": safe_float(
                            row.get("execution_matrix_weight")
                        ),
                        "filter_reasons": list(
                            row.get("matrix_filter_reasons") or []
                        ),
                    }
                    for row in opposing
                ]
                lineage_components = sorted(
                    [
                        {
                            "forecast_id": str(row.get("forecast_id") or ""),
                            "family": str(row.get("family") or ""),
                            "model_id": str(row.get("model_id") or ""),
                            "lane_id": str(row.get("lane_id") or ""),
                            "input_timeframe": str(
                                row.get("input_timeframe") or ""
                            ),
                            "direction": str(row.get("direction") or ""),
                            "signal_role": str(
                                row.get("signal_role") or "structural"
                            ),
                            "account_eligible": bool(
                                row.get("account_eligible", True)
                            ),
                            "forecast_generated_epoch": row.get(
                                "forecast_generated_epoch"
                            ),
                            "signal_candle_time": row.get("signal_candle_time"),
                            "entry_time": row.get("entry_time"),
                        }
                        for row in point.get("contributors") or []
                    ],
                    key=lambda row: json.dumps(
                        row,
                        sort_keys=True,
                        separators=(",", ":"),
                        default=str,
                    ),
                )
                lineage_payload = {
                    "contract_id": AGGREGATE_SIGNAL_LINEAGE_CONTRACT_ID,
                    "instrument": instrument,
                    "horizon_sec": int(point.get("horizon_sec") or 0),
                    "direction": point_direction,
                    "contributors": lineage_components,
                }
                lineage_digest = hashlib.sha256(
                    json.dumps(
                        lineage_payload,
                        sort_keys=True,
                        separators=(",", ":"),
                        default=str,
                    ).encode("utf-8")
                ).hexdigest()
                point.update(
                    {
                        "aggregate_signal_id": f"aggregate_signal_{lineage_digest[:40]}",
                        "aggregate_signal_lineage_contract_id": (
                            AGGREGATE_SIGNAL_LINEAGE_CONTRACT_ID
                        ),
                        "direction_conflict": bool(opposing),
                        "direction_conflict_contract_id": (
                            DIRECTION_CONFLICT_CONTRACT_ID
                        ),
                        "direction_conflict_contributor_count": len(opposing),
                        "direction_conflict_account_eligible_count": sum(
                            bool(row.get("account_eligible", True))
                            for row in opposing
                        ),
                        "direction_conflict_execution_component_count": sum(
                            bool(row.get("matrix_execution_passed"))
                            for row in opposing
                        ),
                        "direction_conflict_shadow_only_count": sum(
                            bool(row.get("research_only"))
                            or not bool(row.get("account_eligible", True))
                            for row in opposing
                        ),
                        "direction_conflict_only_shadow_or_account_ineligible": bool(
                            opposing
                            and all(
                                bool(row.get("research_only"))
                                or not bool(row.get("account_eligible", True))
                                for row in opposing
                            )
                        ),
                        "direction_conflict_contributors": compact_opposing,
                    }
                )
            preferred = max(
                horizon_breakdown,
                key=lambda row: (
                    bool(row.get("signal_eligible")),
                    safe_float(row.get("normalized_rank_score"), -999.0),
                    safe_float(row.get("projected_net_pips_per_hour"), -999.0),
                    safe_float(row.get("signal_confidence")),
                ),
            )
            direction = str(preferred.get("direction") or "")
            representative_pool = [
                row
                for row in components
                if str(row.get("direction") or "") == direction
                and str(row.get("signal_role") or "structural")
                != "entry_exit_timing"
                and row.get("account_eligible", True)
            ]
            if not representative_pool:
                representative_pool = [
                    row
                    for row in components
                    if str(row.get("direction") or "") == direction
                ] or components
            representative = dict(max(representative_pool, key=cls._signal_rank_key))
            representative.update(
                {
                    "direction": direction,
                    "direction_state": str(
                        preferred.get("direction_state") or direction
                    ),
                    "execution_horizon_sec": int(preferred["horizon_sec"]),
                    "preferred_horizon_sec": int(preferred["horizon_sec"]),
                    "signal_eligible": bool(preferred.get("signal_eligible")),
                    "signal_blocked_by": list(preferred.get("signal_blocked_by") or []),
                    "signal_confidence": safe_float(preferred.get("signal_confidence")),
                    "probability_up": safe_float(preferred.get("probability_up"), 0.5),
                    "signal_score": safe_float(preferred.get("signal_score")),
                    "normalized_rank_score": safe_float(
                        preferred.get("normalized_rank_score"), -999.0
                    ),
                    "expected_margin_return_per_hour_pct": safe_float(
                        preferred.get("expected_margin_return_per_hour_pct")
                    ),
                    "confidence_adjusted_margin_return_per_hour_pct": safe_float(
                        preferred.get(
                            "confidence_adjusted_margin_return_per_hour_pct"
                        )
                    ),
                    "projected_net_pips": safe_float(
                        preferred.get("projected_net_pips")
                    ),
                    "projected_net_pips_per_hour": safe_float(
                        preferred.get("projected_net_pips_per_hour")
                    ),
                    "prediction_quality": preferred.get("prediction_quality") or {},
                    "paper_consensus_eligible": bool(
                        preferred.get("paper_consensus_eligible")
                    ),
                    "paper_consensus_evidence": (
                        preferred.get("paper_consensus_evidence") or {}
                    ),
                    "matrix_aggregation_method": preferred.get("aggregation_method"),
                    "aggregate_signal_id": preferred.get("aggregate_signal_id"),
                    "aggregate_signal_lineage_contract_id": preferred.get(
                        "aggregate_signal_lineage_contract_id"
                    ),
                }
            )
            representative.update(
                {
                    "signal_group_id": f"{instrument}:all-signals",
                    "aggregate_signal": True,
                    "component_count": len(components),
                    "component_family_count": len(
                        {str(row.get("family") or "") for row in components}
                    ),
                    "component_families": sorted(
                        {str(row.get("family") or "") for row in components}
                    ),
                    "component_archetype_count": len(
                        {
                            strategy_archetype(
                                row.get("family") or row.get("lane_id")
                            )
                            for row in components
                        }
                    ),
                    "component_archetypes": sorted(
                        {
                            strategy_archetype(
                                row.get("family") or row.get("lane_id")
                            )
                            for row in components
                        }
                    ),
                    "component_models": sorted(
                        {
                            str(row.get("model_id") or row.get("lane_id") or row.get("family") or "")
                            for row in components
                        }
                    ),
                    "component_timeframes": sorted(
                        {str(row.get("input_timeframe") or "multi") for row in components}
                    ),
                    "component_horizons_sec": [
                        int(row["horizon_sec"]) for row in horizon_breakdown
                    ],
                    "horizon_breakdown": horizon_breakdown,
                    "entry_exit_component_count": sum(
                        str(row.get("signal_role") or "") == "entry_exit_timing"
                        for row in components
                    ),
                    "direction_conflict": bool(
                        preferred.get("direction_conflict")
                    ),
                    "direction_conflict_contract_id": preferred.get(
                        "direction_conflict_contract_id"
                    ),
                    "direction_conflict_contributor_count": int(
                        preferred.get("direction_conflict_contributor_count") or 0
                    ),
                    "direction_conflict_account_eligible_count": int(
                        preferred.get("direction_conflict_account_eligible_count")
                        or 0
                    ),
                    "direction_conflict_execution_component_count": int(
                        preferred.get(
                            "direction_conflict_execution_component_count"
                        )
                        or 0
                    ),
                    "direction_conflict_shadow_only_count": int(
                        preferred.get("direction_conflict_shadow_only_count") or 0
                    ),
                    "direction_conflict_only_shadow_or_account_ineligible": bool(
                        preferred.get(
                            "direction_conflict_only_shadow_or_account_ineligible"
                        )
                    ),
                    "direction_conflict_contributors": list(
                        preferred.get("direction_conflict_contributors") or []
                    ),
                    "opposing_direction": "sell" if direction == "buy" else "buy",
                    "opposing_signal_confidence": round(
                        1.0 - safe_float(preferred.get("signal_confidence"), 0.5),
                        6,
                    ),
                }
            )
            consolidated.append(representative)
        consolidated.sort(key=cls._signal_rank_key, reverse=True)
        return consolidated

    def quote_to_account_rate(self, instrument: str, mid: float) -> float | None:
        """Return a verified conversion, or no rate when exposure is unknown.

        Cross-currency cache age includes the source quote's age; a recent
        fetch cannot make an old broker quote current again.
        """
        pair = instrument.split("_")
        if len(pair) != 2 or not all(pair) or not self.account_currency:
            return None
        base_currency, quote_currency = pair
        if quote_currency == self.account_currency:
            return 1.0
        if base_currency == self.account_currency:
            price = safe_float(mid, 0.0)
            return 1.0 / price if price > 0.0 else None
        cached = self.conversion_cache.get(quote_currency)
        if cached:
            cached_rate = safe_float(cached[0], 0.0)
            age = time.monotonic() - safe_float(cached[1], -math.inf)
            if cached_rate > 0.0 and 0.0 <= age <= CONVERSION_QUOTE_MAX_AGE_SEC:
                return cached_rate
            self.conversion_cache.pop(quote_currency, None)
        direct = f"{quote_currency}_{self.account_currency}"
        inverse = f"{self.account_currency}_{quote_currency}"
        for conversion_pair, inverted in ((direct, False), (inverse, True)):
            try:
                quotes = self.client.pricing_snapshot(self.account_id, [conversion_pair])
                quote = quotes.get(conversion_pair)
                bid = safe_float(getattr(quote, "bid", None), 0.0)
                ask = safe_float(getattr(quote, "ask", None), 0.0)
                timestamp = parse_rfc3339(str(getattr(quote, "time", "") or ""))
                if (
                    not bool(getattr(quote, "tradeable", False))
                    or bid <= 0.0 or ask < bid or timestamp is None
                ):
                    continue
                age = (datetime.now(timezone.utc) - timestamp).total_seconds()
                if not 0.0 <= age <= CONVERSION_QUOTE_MAX_AGE_SEC:
                    continue
                quote_mid = (bid + ask) / 2.0
                rate = 1.0 / quote_mid if inverted else quote_mid
                if not math.isfinite(rate) or rate <= 0.0:
                    continue
                self.conversion_cache[quote_currency] = (rate, time.monotonic() - age)
                return rate
            except (OandaApiError, ValueError, TypeError):
                continue
        return None

    def dynamic_sizing(self, candidate: dict[str, Any], summary: dict[str, Any]) -> tuple[int, dict[str, float]]:
        if not bool(getattr(self.args, "execution_dynamic_sizing", True)):
            return int(self.args.execution_units), {"mode": "fixed", "units": int(self.args.execution_units)}
        nav = max(0.0, safe_float(summary.get("NAV"), safe_float(summary.get("balance"))))
        margin_used = max(0.0, safe_float(summary.get("marginUsed")))
        margin_available = max(0.0, safe_float(summary.get("marginAvailable"), nav - margin_used))
        confidence = max(0.5, min(0.99, safe_float(candidate.get("signal_confidence"), 0.5)))
        min_confidence = safe_float(getattr(self.args, "execution_min_signal_confidence", 0.54), 0.54)
        quality = max(0.0, min(1.0, (confidence - min_confidence) / max(0.01, 0.75 - min_confidence)))
        base_target_pct = safe_float(getattr(self.args, "execution_target_margin_used_pct", 55.0), 55.0)
        high_target_pct = safe_float(getattr(self.args, "execution_high_confidence_margin_used_pct", 68.0), 68.0)
        hard_margin_pct = safe_float(getattr(self.args, "execution_hard_margin_used_pct", 75.0), 75.0)
        target_pct = base_target_pct + quality * max(0.0, high_target_pct - base_target_pct)
        per_trade_pct = safe_float(getattr(self.args, "execution_min_trade_margin_pct", 12.0), 12.0) + quality * (
            safe_float(getattr(self.args, "execution_max_trade_margin_pct", 24.0), 24.0)
            - safe_float(getattr(self.args, "execution_min_trade_margin_pct", 12.0), 12.0)
        )
        margin_budget = min(
            margin_available,
            max(0.0, nav * target_pct / 100.0 - margin_used),
            max(0.0, nav * hard_margin_pct / 100.0 - margin_used),
            nav * per_trade_pct / 100.0,
        )
        risk_pct = safe_float(getattr(self.args, "execution_min_risk_pct", 0.8), 0.8) + quality * (
            safe_float(getattr(self.args, "execution_max_risk_pct", 2.5), 2.5)
            - safe_float(getattr(self.args, "execution_min_risk_pct", 0.8), 0.8)
        )
        risk_budget = nav * risk_pct / 100.0
        instrument = str(candidate.get("instrument") or "")
        mid = (safe_float(candidate.get("bid")) + safe_float(candidate.get("ask"))) / 2.0
        pip = max(1e-12, safe_float(candidate.get("pip"), 0.0001))
        conversion = self.quote_to_account_rate(instrument, mid)
        if conversion is None or not math.isfinite(conversion) or conversion <= 0.0:
            return 0, {"mode": "blocked", "reason": "account_conversion_unavailable", "units": 0}
        meta = self.instrument_meta.get(instrument) or {}
        margin_rate = max(1e-6, safe_float(meta.get("margin_rate"), safe_float(summary.get("marginRate"), 0.02)))
        stop_pips = max(0.1, safe_float(candidate.get("stop_loss_pips"), 5.0))
        risk_per_unit = stop_pips * pip * conversion
        margin_per_unit = max(1e-9, mid * conversion * margin_rate)
        risk_units = int(risk_budget / max(1e-9, risk_per_unit))
        margin_units = int(margin_budget / margin_per_unit)
        max_units = int(safe_float(getattr(self.args, "execution_max_units", 5000), 5000))
        units = max(0, min(risk_units, margin_units, max_units))
        minimum_units = max(1, int(safe_float(meta.get("minimum_trade_size"), 1.0)))
        if units < minimum_units:
            units = 0
        return units, {
            "mode": "confidence_risk_margin",
            "nav": round(nav, 6),
            "confidence": round(confidence, 6),
            "quality": round(quality, 6),
            "risk_pct": round(risk_pct, 4),
            "risk_budget": round(risk_budget, 6),
            "target_margin_pct": round(target_pct, 4),
            "hard_margin_pct": round(hard_margin_pct, 4),
            "margin_used": round(margin_used, 6),
            "margin_budget": round(margin_budget, 6),
            "estimated_margin": round(units * margin_per_unit, 6),
            "estimated_stop_risk": round(units * risk_per_unit, 6),
            "risk_limited_units": risk_units,
            "margin_limited_units": margin_units,
            "units": units,
        }

    def portfolio_blocker(self, candidate: dict[str, Any], trades: list[dict[str, Any]]) -> str:
        max_positions = int(safe_float(getattr(self.args, "execution_max_open_positions", 1), 1))
        if len(trades) >= max_positions:
            return "max_open_positions"
        instrument = str(candidate.get("instrument") or "")
        if any(str(trade.get("instrument") or "") == instrument for trade in trades):
            return "duplicate_instrument"
        max_currency = int(safe_float(getattr(self.args, "execution_max_currency_direction_positions", 3), 3))
        if max_currency <= 0:
            return ""
        exposures: Counter[tuple[str, int]] = Counter()
        for trade in trades:
            pair = str(trade.get("instrument") or "").split("_")
            if len(pair) != 2:
                continue
            direction = 1 if safe_float(trade.get("currentUnits")) > 0.0 else -1
            exposures[(pair[0], direction)] += 1
            exposures[(pair[1], -direction)] += 1
        pair = instrument.split("_")
        direction = 1 if str(candidate.get("direction")) == "buy" else -1
        if len(pair) == 2 and (
            exposures[(pair[0], direction)] >= max_currency
            or exposures[(pair[1], -direction)] >= max_currency
        ):
            return "currency_direction_concentration"
        return ""

    def final_submission_blocker(
        self,
        candidate: dict[str, Any],
        units: int,
        order_body: dict[str, Any],
    ) -> str:
        """Last fail-closed hook immediately before the broker write."""
        return ""

    def reentry_blocker(
        self,
        candidate: dict[str, Any],
        trades: list[dict[str, Any]],
    ) -> tuple[str, dict[str, Any]]:
        instrument = str(candidate.get("instrument") or "")
        direction = str(candidate.get("direction") or "").lower()
        if bool(candidate.get("direction_conflict")):
            return "direction_conflict_shadow_only", {
                "instrument": instrument,
                "direction": direction,
            }
        now_epoch = time.time()
        pair_key = f"{instrument}|{direction}"
        checks = (
            (
                "same_direction_reentry_cooldown",
                self.recent_pair_direction_exits.get(pair_key),
                safe_float(
                    getattr(self.args, "execution_reentry_cooldown_sec", 900.0),
                    900.0,
                ),
            ),
            (
                "instrument_reentry_cooldown",
                self.recent_instrument_exits.get(instrument),
                safe_float(
                    getattr(
                        self.args,
                        "execution_instrument_reentry_cooldown_sec",
                        300.0,
                    ),
                    300.0,
                ),
            ),
        )
        for reason, exited_epoch, cooldown_sec in checks:
            if exited_epoch is None or cooldown_sec <= 0.0:
                continue
            age = max(0.0, now_epoch - exited_epoch)
            if age < cooldown_sec:
                return reason, {
                    "instrument": instrument,
                    "direction": direction,
                    "exit_age_sec": round(age, 3),
                    "cooldown_sec": cooldown_sec,
                    "remaining_sec": round(cooldown_sec - age, 3),
                }
        factor = self.jpy_factor_key(instrument, direction)
        factor_cooldown = safe_float(
            getattr(self.args, "execution_jpy_factor_cooldown_sec", 900.0),
            900.0,
        )
        factor_exit = self.recent_factor_exits.get(factor) if factor else None
        if factor_exit is not None and factor_cooldown > 0.0:
            age = max(0.0, now_epoch - factor_exit)
            if age < factor_cooldown:
                return "jpy_factor_reentry_cooldown", {
                    "instrument": instrument,
                    "direction": direction,
                    "correlation_factor": factor,
                    "exit_age_sec": round(age, 3),
                    "cooldown_sec": factor_cooldown,
                    "remaining_sec": round(factor_cooldown - age, 3),
                }
        max_factor_positions = max(
            0,
            int(
                safe_float(
                    getattr(
                        self.args,
                        "execution_max_open_jpy_factor_positions",
                        1,
                    ),
                    1,
                )
            ),
        )
        if factor and max_factor_positions > 0:
            open_factor_count = 0
            for trade in trades:
                trade_instrument = str(trade.get("instrument") or "")
                trade_direction = (
                    "buy" if safe_float(trade.get("currentUnits")) > 0.0 else "sell"
                )
                if self.jpy_factor_key(trade_instrument, trade_direction) == factor:
                    open_factor_count += 1
            if open_factor_count >= max_factor_positions:
                return "jpy_factor_position_capacity", {
                    "instrument": instrument,
                    "direction": direction,
                    "correlation_factor": factor,
                    "open_factor_positions": open_factor_count,
                    "max_factor_positions": max_factor_positions,
                }
        return "", {
            "instrument": instrument,
            "direction": direction,
            "correlation_factor": factor,
        }

    def cost_capture_blocker(
        self,
        candidate: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        """Require forecasts to predict enough movement to clear their costs."""
        horizon = int(safe_float(candidate.get("execution_horizon_sec")))
        maximum_horizon = int(
            safe_float(
                getattr(
                    self.args,
                    "execution_intrahour_cost_gate_max_horizon_sec",
                    3600,
                ),
                3600,
            )
        )
        if horizon <= 0:
            return "", {"applied": False, "horizon_sec": horizon}
        spread = max(0.0, safe_float(candidate.get("spread_pips")))
        confidence = safe_float(candidate.get("signal_confidence"), 0.5)
        # ``projected_net_pips`` may include a calibrated historical lane
        # expectancy.  It can legitimately differ from today's forecast, but
        # it must not satisfy the live movement/cost gate when the current
        # executable projection itself does not clear costs.
        calibrated_projected_net = safe_float(candidate.get("projected_net_pips"))
        instant_projected_net = (
            safe_float(candidate.get("instant_projected_net_pips"))
            if candidate.get("instant_projected_net_pips") is not None
            else calibrated_projected_net
        )
        gross_to_spread = safe_float(candidate.get("gross_to_spread"))
        projected_gross = safe_float(
            candidate.get("projected_gross_movement_pips")
        )
        if gross_to_spread <= 0.0 and spread > 0.0 and projected_gross > 0.0:
            gross_to_spread = projected_gross / spread
        if projected_gross <= 0.0 and gross_to_spread > 0.0:
            projected_gross = gross_to_spread * spread
        liquidity_quality = safe_float(candidate.get("liquidity_quality"))
        intrahour = horizon <= maximum_horizon
        gate_name = "intrahour" if intrahour else "multihour"
        enabled = bool(
            getattr(
                self.args,
                (
                    "execution_intrahour_cost_gate"
                    if intrahour
                    else "execution_multihour_cost_gate"
                ),
                True,
            )
        )
        if not enabled:
            return "", {
                "applied": False,
                "gate": gate_name,
                "horizon_sec": horizon,
            }
        if intrahour:
            max_spread = safe_float(
                getattr(self.args, "execution_intrahour_max_spread_pips", 2.0),
                2.0,
            )
            min_liquidity = safe_float(
                getattr(
                    self.args,
                    "execution_intrahour_min_liquidity_quality",
                    0.70,
                ),
                0.70,
            )
            min_confidence = safe_float(
                getattr(self.args, "execution_intrahour_min_confidence", 0.56),
                0.56,
            )
            min_gross_to_spread = safe_float(
                getattr(
                    self.args,
                    "execution_intrahour_min_gross_to_spread",
                    2.5,
                ),
                2.5,
            )
            min_after_cost = safe_float(
                getattr(
                    self.args,
                    "execution_intrahour_min_after_cost_pips",
                    1.0,
                ),
                1.0,
            )
        else:
            max_spread = safe_float(
                getattr(self.args, "execution_multihour_max_spread_pips", 3.0),
                3.0,
            )
            min_liquidity = safe_float(
                getattr(
                    self.args,
                    "execution_multihour_min_liquidity_quality",
                    0.65,
                ),
                0.65,
            )
            min_confidence = safe_float(
                getattr(self.args, "execution_multihour_min_confidence", 0.55),
                0.55,
            )
            min_gross_to_spread = safe_float(
                getattr(
                    self.args,
                    "execution_multihour_min_gross_to_spread",
                    2.0,
                ),
                2.0,
            )
            min_after_cost = safe_float(
                getattr(
                    self.args,
                    "execution_multihour_min_after_cost_pips",
                    1.0,
                ),
                1.0,
            )
        policy = {
            "applied": True,
            "gate": gate_name,
            "horizon_sec": horizon,
            "spread_pips": round(spread, 4),
            "signal_confidence": round(confidence, 6),
            "projected_net_pips": round(calibrated_projected_net, 4),
            "instant_projected_net_pips": round(instant_projected_net, 4),
            "cost_gate_after_cost_pips": round(instant_projected_net, 4),
            "projected_gross_movement_pips": round(projected_gross, 4),
            "gross_to_spread": round(gross_to_spread, 4),
            "liquidity_quality": round(liquidity_quality, 4),
            "max_spread_pips": max_spread,
            "min_liquidity_quality": min_liquidity,
            "min_confidence": min_confidence,
            "min_gross_to_spread": min_gross_to_spread,
            "min_after_cost_pips": min_after_cost,
        }
        checks = (
            (
                f"{gate_name}_spread_bucket",
                spread <= policy["max_spread_pips"],
            ),
            (
                f"{gate_name}_liquidity_quality",
                liquidity_quality >= policy["min_liquidity_quality"],
            ),
            (
                f"{gate_name}_direction_confidence",
                confidence >= policy["min_confidence"],
            ),
            (
                f"{gate_name}_movement_to_cost",
                gross_to_spread >= policy["min_gross_to_spread"],
            ),
            (
                f"{gate_name}_after_cost_edge",
                instant_projected_net >= policy["min_after_cost_pips"],
            ),
        )
        for reason, passed in checks:
            if not passed:
                return reason, policy
        return "", policy

    def apply_horizon_risk_shape(self, candidate: dict[str, Any]) -> dict[str, Any]:
        """Scale protective distance for forecasts whose horizon is not a scalp."""
        row = candidate
        horizon = int(
            safe_float(
                row.get("execution_exit_horizon_sec"),
                safe_float(row.get("execution_horizon_sec")),
            )
        )
        original_stop = max(0.1, safe_float(row.get("stop_loss_pips"), 5.0))
        if (
            not bool(getattr(self.args, "execution_horizon_scaled_protection", True))
            or horizon <= 3600
        ):
            shape = {
                "applied": False,
                "horizon_sec": horizon,
                "original_stop_pips": original_stop,
                "effective_stop_pips": original_stop,
            }
            row["horizon_risk_shape"] = shape
            return shape
        if horizon <= 7200:
            base_floor, cap = 8.0, 16.0
        elif horizon <= 14400:
            base_floor, cap = 10.0, 20.0
        elif horizon <= 43200:
            base_floor, cap = 12.0, 24.0
        else:
            base_floor, cap = 15.0, 30.0
        spread = max(0.0, safe_float(row.get("spread_pips")))
        atr = max(0.0, safe_float(row.get("atr_pips")))
        projected_gross = max(
            0.0,
            safe_float(row.get("projected_gross_movement_pips")),
        )
        evidence_floor = max(
            base_floor,
            3.0 * spread,
            0.25 * atr if atr > 0.0 else 0.0,
            0.15 * projected_gross if projected_gross > 0.0 else 0.0,
        )
        computed_floor = min(cap, evidence_floor)
        effective_stop = max(original_stop, computed_floor)
        row["stop_loss_pips"] = round(effective_stop, 3)
        shape = {
            "applied": True,
            "horizon_sec": horizon,
            "original_stop_pips": round(original_stop, 3),
            "base_floor_pips": base_floor,
            "spread_floor_pips": round(3.0 * spread, 3),
            "atr_floor_pips": round(0.25 * atr, 3),
            "projection_floor_pips": round(0.15 * projected_gross, 3),
            "cap_pips": cap,
            "effective_stop_pips": round(effective_stop, 3),
            "risk_normalized_sizing": True,
        }
        row["horizon_risk_shape"] = shape
        return shape

    def reentry_guard_summary(self) -> dict[str, Any]:
        now_epoch = time.time()

        def ages(values: dict[str, float]) -> dict[str, float]:
            return {
                key: round(max(0.0, now_epoch - epoch), 3)
                for key, epoch in sorted(values.items())
            }

        return {
            "state_path": str(self.reentry_state_path),
            "pair_direction_cooldown_sec": safe_float(
                getattr(self.args, "execution_reentry_cooldown_sec", 900.0),
                900.0,
            ),
            "instrument_cooldown_sec": safe_float(
                getattr(
                    self.args,
                    "execution_instrument_reentry_cooldown_sec",
                    300.0,
                ),
                300.0,
            ),
            "jpy_factor_cooldown_sec": safe_float(
                getattr(
                    self.args,
                    "execution_jpy_factor_cooldown_sec",
                    900.0,
                ),
                900.0,
            ),
            "pair_direction_exit_ages_sec": ages(
                self.recent_pair_direction_exits
            ),
            "instrument_exit_ages_sec": ages(self.recent_instrument_exits),
            "factor_exit_ages_sec": ages(self.recent_factor_exits),
            "owned_open_trade_count": len(self.owned_trade_state),
        }

    def _record_order_telemetry(self, row: dict[str, Any]) -> dict[str, Any]:
        telemetry = {
            "time": utc_now(),
            **row,
        }
        self.order_telemetry.append(telemetry)
        self.last_order_telemetry = telemetry
        return telemetry

    def execution_telemetry_summary(self) -> dict[str, Any]:
        rows = list(self.order_telemetry)
        latencies = [
            safe_float(row.get("order_roundtrip_ms"))
            for row in rows
            if row.get("order_roundtrip_ms") is not None
        ]
        slippages = [
            safe_float(row.get("adverse_slippage_pips"))
            for row in rows
            if row.get("adverse_slippage_pips") is not None
        ]
        missed = [
            safe_float(row.get("missed_entry_move_pips"))
            for row in rows
            if row.get("missed_entry_move_pips") is not None
        ]
        status_counts = Counter(str(row.get("status") or "unknown") for row in rows)
        p95_latency = None
        if latencies:
            ordered = sorted(latencies)
            p95_latency = ordered[min(len(ordered) - 1, math.ceil(0.95 * len(ordered)) - 1)]
        return {
            "sample_count": len(rows),
            "status_counts": dict(sorted(status_counts.items())),
            "avg_order_roundtrip_ms": (
                None if not latencies else round(statistics.fmean(latencies), 3)
            ),
            "p95_order_roundtrip_ms": (
                None if p95_latency is None else round(p95_latency, 3)
            ),
            "avg_adverse_slippage_pips": (
                None if not slippages else round(statistics.fmean(slippages), 4)
            ),
            "avg_missed_entry_move_pips": (
                None if not missed else round(statistics.fmean(missed), 4)
            ),
            "latest": self.last_order_telemetry,
        }

    def portfolio_margin_blocker(
        self,
        candidate: dict[str, Any],
        trades: list[dict[str, Any]],
        summary: dict[str, Any],
        sizing: dict[str, float],
    ) -> tuple[str, dict[str, Any]]:
        maximum_pct = safe_float(
            getattr(
                self.args,
                "execution_max_currency_direction_margin_pct",
                45.0,
            ),
            45.0,
        )
        nav = max(
            0.0,
            safe_float(summary.get("NAV"), safe_float(summary.get("balance"))),
        )
        if maximum_pct <= 0.0 or nav <= 0.0:
            return "", {}
        exposures: defaultdict[tuple[str, int], float] = defaultdict(float)
        for trade in trades:
            pair = str(trade.get("instrument") or "").split("_")
            if len(pair) != 2:
                continue
            margin = max(0.0, safe_float(trade.get("initialMargin")))
            if margin <= 0.0:
                continue
            direction = 1 if safe_float(trade.get("currentUnits")) > 0.0 else -1
            exposures[(pair[0], direction)] += margin
            exposures[(pair[1], -direction)] += margin
        pair = str(candidate.get("instrument") or "").split("_")
        if len(pair) != 2:
            return "", {}
        direction = 1 if str(candidate.get("direction") or "") == "buy" else -1
        estimated_margin = max(0.0, safe_float(sizing.get("estimated_margin")))
        candidate_keys = ((pair[0], direction), (pair[1], -direction))
        projected = {
            f"{currency}:{'long' if side > 0 else 'short'}": round(
                100.0 * (exposures[(currency, side)] + estimated_margin) / nav,
                4,
            )
            for currency, side in candidate_keys
        }
        maximum_projected = max(projected.values(), default=0.0)
        details = {
            "maximum_pct": round(maximum_pct, 4),
            "maximum_projected_pct": round(maximum_projected, 4),
            "candidate_estimated_margin": round(estimated_margin, 6),
            "projected_currency_direction_margin_pct": projected,
        }
        if maximum_projected > maximum_pct:
            return "currency_direction_margin_concentration", details
        return "", details

    def second_curve_state(self, instrument: str, direction: str) -> dict[str, Any]:
        """Summarize the fresh S1 curve as timing evidence for one trade side."""

        path = Path(
            getattr(
                self.args,
                "execution_second_forecast_state",
                DEFAULT_LOG_DIR.parent / "state" / "second_forecast_hot_v1.json",
            )
        )
        now = time.monotonic()
        try:
            stat = path.stat()
        except OSError:
            return {"state": "unavailable", "points": 0}
        max_age = safe_float(
            getattr(self.args, "execution_second_curve_max_age_sec", 30.0),
            30.0,
        )
        age_sec = max(0.0, time.time() - stat.st_mtime)
        if age_sec > max_age:
            return {"state": "stale", "points": 0, "age_sec": round(age_sec, 3)}
        if now - self.second_curve_cache_at >= 0.5 or stat.st_mtime_ns != self.second_curve_cache_mtime_ns:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                return {"state": "unavailable", "points": 0}
            grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
            curves = payload.get("forecast_curves") or [] if isinstance(payload, dict) else []
            for curve in curves:
                pair = str(curve.get("instrument") or "")
                grouped[pair].extend(
                    point for point in curve.get("points") or [] if isinstance(point, dict)
                )
            if not grouped and isinstance(payload, dict):
                for point in payload.get("top_forecasts") or []:
                    if isinstance(point, dict):
                        grouped[str(point.get("instrument") or "")].append(point)
            self.second_curve_cache = dict(grouped)
            self.second_curve_cache_at = now
            self.second_curve_cache_mtime_ns = stat.st_mtime_ns
        sign = 1.0 if direction == "buy" else -1.0
        max_horizon = int(
            safe_float(getattr(self.args, "execution_second_curve_max_horizon_sec", 300), 300)
        )
        points: list[dict[str, Any]] = []
        for point in self.second_curve_cache.get(instrument) or []:
            horizon = int(safe_float(point.get("horizon_sec")))
            if horizon <= 0 or horizon > max_horizon:
                continue
            probability_up = safe_float(point.get("probability_up"), 0.5)
            directional_pips = sign * safe_float(point.get("predicted_signed_pips"))
            spread_pips = max(0.0, safe_float(point.get("spread_pips")))
            points.append(
                {
                    "horizon_sec": horizon,
                    "side_probability": probability_up if direction == "buy" else 1.0 - probability_up,
                    "directional_pips": directional_pips,
                    "directional_net_pips": directional_pips - spread_pips,
                    "spread_pips": spread_pips,
                    "historical_gate_passed": bool(point.get("historical_gate_passed")),
                    "weight": 1.0 / math.sqrt(max(15, horizon)),
                }
            )
        minimum_points = int(
            safe_float(getattr(self.args, "execution_second_curve_min_points", 3), 3)
        )
        if len(points) < minimum_points:
            return {
                "state": "insufficient",
                "points": len(points),
                "age_sec": round(age_sec, 3),
            }
        total_weight = sum(point["weight"] for point in points)
        probability = sum(
            point["weight"] * point["side_probability"] for point in points
        ) / total_weight
        directional_pips = sum(
            point["weight"] * point["directional_pips"] for point in points
        ) / total_weight
        directional_net_pips = sum(
            point["weight"] * point["directional_net_pips"] for point in points
        ) / total_weight
        average_spread_pips = sum(
            point["weight"] * point["spread_pips"] for point in points
        ) / total_weight
        oppose_probability = safe_float(
            getattr(self.args, "execution_second_curve_oppose_probability", 0.47),
            0.47,
        )
        align_probability = 1.0 - oppose_probability
        minimum_net_pips = safe_float(
            getattr(self.args, "execution_second_curve_min_net_pips", 0.05),
            0.05,
        )
        aligned_fraction = sum(
            point["side_probability"] >= align_probability
            and point["directional_net_pips"] >= minimum_net_pips
            for point in points
        ) / len(points)
        minimum_aligned_fraction = safe_float(
            getattr(self.args, "execution_second_curve_min_aligned_fraction", 0.5),
            0.5,
        )
        if probability <= oppose_probability and directional_pips < 0.0:
            state = "opposed"
        elif directional_net_pips < minimum_net_pips:
            state = "cost_blocked"
        elif (
            probability >= align_probability
            and directional_pips > 0.0
            and aligned_fraction >= minimum_aligned_fraction
        ):
            state = "aligned"
        else:
            state = "neutral"
        return {
            "state": state,
            "points": len(points),
            "age_sec": round(age_sec, 3),
            "side_probability": round(probability, 6),
            "directional_pips": round(directional_pips, 4),
            "directional_net_pips": round(directional_net_pips, 4),
            "spread_pips": round(average_spread_pips, 4),
            "minimum_net_pips": round(minimum_net_pips, 4),
            "aligned_fraction": round(aligned_fraction, 6),
            "historically_gated_fraction": round(
                sum(point["historical_gate_passed"] for point in points) / len(points),
                6,
            ),
            "max_horizon_sec": max_horizon,
        }

    def manage_open_trades(
        self,
        *,
        force: bool = False,
        prices: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        now = time.monotonic()
        if not force and not self.has_owned_trade:
            return []
        if not force and now < self.next_manage_monotonic:
            return []
        self.next_manage_monotonic = now + self.args.execution_manage_interval_sec
        trades = self.open_trades()
        if not bool(getattr(self.args, "execution_manage_trades", True)):
            self._observe_owned_trades(trades)
            return trades
        owned = [trade for trade in trades if self.owns_trade(trade)]
        instruments = sorted({str(trade.get("instrument") or "") for trade in owned if trade.get("instrument")})
        quotes = {
            instrument: prices[instrument]
            for instrument in instruments
            if prices is not None and instrument in prices
        }
        missing_instruments = [instrument for instrument in instruments if instrument not in quotes]
        if missing_instruments:
            try:
                quotes.update(self.client.pricing_snapshot(self.account_id, missing_instruments))
            except OandaApiError:
                pass
        current_utc = datetime.now(timezone.utc)
        changed_trade = False
        for trade in owned:
            opened = parse_rfc3339(str(trade.get("openTime") or ""))
            age = (current_utc - opened).total_seconds() if opened else 0.0
            extensions = trade.get("clientExtensions") or {}
            comment = str(extensions.get("comment") or "")
            horizon_match = re.search(r"\|h(\d+)(?:\||$)", comment)
            trade_horizon = int(horizon_match.group(1)) if horizon_match else 0
            base_hold_sec = (
                trade_horizon
                if trade_horizon in set(self.args.execution_horizons)
                else self.args.execution_max_hold_sec
            )
            trade_id = str(trade.get("id") or "")
            if not trade_id:
                continue
            instrument = str(trade.get("instrument") or "")
            pip_location = self.instrument_meta.get(instrument, {}).get("pip_location")
            pip = 10.0 ** int(pip_location) if pip_location is not None else (0.01 if instrument.endswith("_JPY") else 0.0001)
            quote = quotes.get(instrument)
            entry = safe_float(trade.get("price"))
            units = safe_float(trade.get("currentUnits"))
            current_pips = 0.0
            if quote is not None and pip > 0.0:
                current_pips = (
                    (safe_float(quote.bid) - entry) / pip
                    if units > 0.0
                    else (entry - safe_float(quote.ask)) / pip
                )
            second_curve_profit_exit_enabled = bool(
                getattr(self.args, "execution_second_curve_profit_exit", False)
            )
            second_timing = (
                self.second_curve_state(
                    instrument,
                    "buy" if units > 0.0 else "sell",
                )
                if second_curve_profit_exit_enabled
                else {"state": "disabled"}
            )
            trailing_activation = self._comment_number(comment, "ta")
            trailing_distance = self._comment_number(comment, "td")
            trailing_order = trade.get("trailingStopLossOrder") or {}
            if (
                not trailing_order
                and trailing_activation > 0.0
                and trailing_distance > 0.0
                and current_pips >= trailing_activation
            ):
                descriptor = self.acquire_execution_lock(timeout_sec=0.5)
                if descriptor is not None:
                    try:
                        meta = self.instrument_meta.get(instrument) or {}
                        precision = int(
                            safe_float(meta.get("display_precision"), price_precision(pip))
                        )
                        distance, effective_trailing_pips = normalized_trailing_distance(
                            trailing_distance,
                            pip,
                            safe_float(meta.get("minimum_trailing_stop_distance")),
                            precision,
                        )
                        payload = self.client.write(
                            "PUT",
                            f"/v3/accounts/{self.account_id}/trades/{trade_id}/orders",
                            {
                                "trailingStopLoss": {
                                    "distance": f"{distance:.{precision}f}",
                                    "timeInForce": "GTC",
                                }
                            },
                        )
                        log_line(
                            self.log_path,
                            "practice_trailing_activated",
                            trade_id=trade_id,
                            instrument=instrument,
                            age_sec=round(age, 1),
                            current_pips=round(current_pips, 3),
                            activation_pips=trailing_activation,
                            configured_trailing_distance_pips=trailing_distance,
                            effective_trailing_distance_pips=round(effective_trailing_pips, 3),
                            broker_minimum_distance=safe_float(
                                meta.get("minimum_trailing_stop_distance")
                            ),
                            request_id=payload.get("_requestID"),
                        )
                        trailing_order = {"state": "PENDING"}
                        changed_trade = True
                    except OandaApiError as exc:
                        log_line(
                            self.log_path,
                            "practice_trailing_error",
                            trade_id=trade_id,
                            status=exc.status,
                            error=str(exc)[:500],
                        )
                    finally:
                        self.release_execution_lock(descriptor)

            if not trailing_order and current_pips > 0.0 and quote is not None:
                meta = self.instrument_meta.get(instrument) or {}
                precision = int(
                    safe_float(meta.get("display_precision"), price_precision(pip))
                )
                spread_pips = max(
                    0.0,
                    (safe_float(quote.ask) - safe_float(quote.bid)) / pip,
                )
                stop_order = trade.get("stopLossOrder") or {}
                profit_lock = profit_lock_stop_price(
                    "buy" if units > 0.0 else "sell",
                    entry,
                    current_pips,
                    pip,
                    trailing_distance,
                    safe_float(
                        getattr(self.args, "execution_profit_lock_trigger_pips", 2.0),
                        2.0,
                    ),
                    safe_float(
                        getattr(self.args, "execution_profit_lock_floor_pips", 0.2),
                        0.2,
                    ),
                    spread_pips,
                    safe_float(
                        getattr(self.args, "execution_profit_lock_spread_multiple", 1.5),
                        1.5,
                    ),
                    stop_order.get("price"),
                    precision,
                    safe_float(
                        getattr(self.args, "execution_profit_lock_step_pips", 0.25),
                        0.25,
                    ),
                )
                if profit_lock is not None:
                    descriptor = self.acquire_execution_lock(timeout_sec=0.5)
                    if descriptor is not None:
                        try:
                            payload = self.client.write(
                                "PUT",
                                f"/v3/accounts/{self.account_id}/trades/{trade_id}/orders",
                                {
                                    "stopLoss": {
                                        "price": f"{profit_lock['price']:.{precision}f}",
                                        "timeInForce": "GTC",
                                    }
                                },
                            )
                            log_line(
                                self.log_path,
                                "practice_profit_lock_updated",
                                trade_id=trade_id,
                                instrument=instrument,
                                age_sec=round(age, 1),
                                current_pips=round(current_pips, 3),
                                spread_pips=round(spread_pips, 3),
                                locked_pips=round(profit_lock["lock_pips"], 3),
                                previous_locked_pips=(
                                    None
                                    if not math.isfinite(profit_lock["existing_lock_pips"])
                                    else round(profit_lock["existing_lock_pips"], 3)
                                ),
                                market_buffer_pips=round(
                                    profit_lock["market_buffer_pips"],
                                    3,
                                ),
                                stop_price=profit_lock["price"],
                                request_id=payload.get("_requestID"),
                            )
                            trade["stopLossOrder"] = {
                                "price": f"{profit_lock['price']:.{precision}f}"
                            }
                            changed_trade = True
                        except OandaApiError as exc:
                            log_line(
                                self.log_path,
                                "practice_profit_lock_error",
                                trade_id=trade_id,
                                status=exc.status,
                                error=str(exc)[:500],
                            )
                        finally:
                            self.release_execution_lock(descriptor)

            profit_hold_multiplier = max(1.0, safe_float(getattr(self.args, "execution_profit_hold_multiplier", 2.0), 2.0))
            hard_hold_sec = base_hold_sec * profit_hold_multiplier
            extend_profitable = bool(trailing_order and current_pips > 0.0 and age < hard_hold_sec)
            second_curve_exit = bool(
                second_curve_profit_exit_enabled
                and age >= safe_float(
                    getattr(self.args, "execution_second_curve_min_trade_age_sec", 10.0),
                    10.0,
                )
                and current_pips >= safe_float(
                    getattr(self.args, "execution_second_curve_profit_exit_pips", 0.3),
                    0.3,
                )
                and second_timing.get("state") == "opposed"
            )
            if not second_curve_exit and (age < base_hold_sec or extend_profitable):
                continue
            descriptor = self.acquire_execution_lock(timeout_sec=0.5)
            if descriptor is None:
                continue
            try:
                payload = self.client.write(
                    "PUT",
                    f"/v3/accounts/{self.account_id}/trades/{trade_id}/close",
                    {"units": "ALL"},
                )
                fill = payload.get("orderFillTransaction") or {}
                log_line(
                    self.log_path,
                    "practice_trade_closed",
                    trade_id=trade_id,
                    instrument=instrument,
                    age_sec=round(age, 1),
                    execution_horizon_sec=base_hold_sec,
                    hard_hold_sec=hard_hold_sec,
                    current_pips=round(current_pips, 3),
                    realized_pl=safe_float(fill.get("pl")),
                    price=safe_float(fill.get("price")),
                    reason=(
                        "second_curve_profit_reversal"
                        if second_curve_exit
                        else "forecast_horizon" if age < hard_hold_sec else "extended_profit_horizon"
                    ),
                    second_curve=second_timing,
                    request_id=payload.get("_requestID"),
                )
                changed_trade = True
            except OandaApiError as exc:
                self.disabled_reason = "uncertain_close" if exc.outcome_uncertain else self.disabled_reason
                log_line(
                    self.log_path,
                    "practice_close_error",
                    trade_id=trade_id,
                    status=exc.status,
                    outcome_uncertain=exc.outcome_uncertain,
                    error=str(exc)[:500],
                )
            finally:
                self.release_execution_lock(descriptor)
        remaining = self.open_trades() if changed_trade else trades
        self._observe_owned_trades(remaining)
        return remaining

    def ranked_candidate(self, candidates: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
        rank_started = time.perf_counter()
        last_stage = rank_started
        rank_timings: dict[str, float] = {}

        def mark_stage(name: str) -> None:
            nonlocal last_stage
            now = time.perf_counter()
            rank_timings[name] = round(now - last_stage, 6)
            last_stage = now

        candidate_pool = list(candidates)
        if self.signal_feed is not None:
            # Every signal remains visible to the research consensus. Setup,
            # model-quality, cost, and account gates are retained in payloads
            # and applied after all producers have been merged.
            feed_error = ""
            if not bool(
                getattr(
                    self.args,
                    "execution_feed_consumer_only",
                    False,
                )
            ):
                try:
                    feed_publish_pool = self.hot_feed_publish_candidates(
                        candidate_pool
                    )
                    self.signal_feed.publish(
                        feed_publish_pool,
                        str(
                            getattr(
                                self.args,
                                "execution_feed_source",
                                "strategy_lab",
                            )
                        ),
                        safe_float(
                            getattr(
                                self.args,
                                "execution_signal_feed_ttl_sec",
                                8.0,
                            ),
                            8.0,
                        ),
                    )
                except sqlite3.OperationalError as exc:
                    self.signal_feed.connection.rollback()
                    feed_error = f"{type(exc).__name__}: {exc}"
            mark_stage("feed_publish_sec")
            shared_candidates: list[dict[str, Any]] = []
            feed_consumer_only = bool(
                getattr(
                    self.args,
                    "execution_feed_consumer_only",
                    False,
                )
            )
            if feed_consumer_only and self.signal_feed_cache is not None:
                shared_candidates, cache_stats = (
                    self.signal_feed_cache.snapshot()
                )
                self.last_feed_cache_stats = cache_stats
                cache_state = str(cache_stats.get("state") or "unknown")
                if cache_state != self.last_feed_cache_state:
                    log_line(
                        self.log_path,
                        "signal_feed_cache_state",
                        **cache_stats,
                    )
                    self.last_feed_cache_state = cache_state
                if not bool(cache_stats.get("fresh")):
                    feed_error = f"async_cache_{cache_state}"
            elif not feed_error:
                try:
                    shared_candidates = self.signal_feed.recent(
                        int(
                            safe_float(
                                getattr(
                                    self.args,
                                    "execution_signal_feed_limit",
                                    50_000,
                                ),
                                50_000,
                            )
                        )
                    )
                except sqlite3.OperationalError as exc:
                    self.signal_feed.connection.rollback()
                    feed_error = f"{type(exc).__name__}: {exc}"
            mark_stage("feed_recent_sec")
            self.last_feed_candidate_count = len(shared_candidates)
            if feed_consumer_only:
                shared_candidates = self.execution_candidate_prefilter(
                    shared_candidates
                )
            self.last_execution_prefilter_count = len(shared_candidates)
            if feed_error and not feed_consumer_only:
                # The shared feed is advisory. Continue with fresh local
                # candidates and fail closed on external-only signals rather
                # than freezing quote management behind SQLite contention.
                log_line(
                    self.log_path,
                    "signal_feed_contention",
                    error=feed_error[:300],
                    policy="local_candidates_only_external_signals_fail_closed",
                )
            candidate_pool = self.merge_signal_candidates(
                candidate_pool,
                shared_candidates,
            )
            mark_stage("feed_merge_sec")
        else:
            self.last_feed_candidate_count = len(candidate_pool)
            self.last_execution_prefilter_count = len(candidate_pool)
        # Reprice and enrich after the merge so independently produced model
        # signals use the same current spread, depth, margin, and liquidity data.
        candidate_pool = self.refresh_candidate_quotes(candidate_pool)
        mark_stage("quote_refresh_sec")
        candidate_pool = self.enrich_candidate_economics(candidate_pool)
        mark_stage("economics_sec")
        candidate_pool = self.add_signal_agreement(candidate_pool)
        mark_stage("agreement_sec")
        if self.promotion is not None and hasattr(self.promotion, "rank_signal_candidates"):
            ranked_candidates = self.promotion.rank_signal_candidates(
                candidate_pool,
                min_confidence=safe_float(getattr(self.args, "execution_min_signal_confidence", 0.54), 0.54),
                min_expected_net_pips=safe_float(
                    getattr(self.args, "execution_min_signal_expected_net_pips", 0.05),
                    0.05,
                ),
            )
            mark_stage("promotion_rank_sec")
            for candidate in ranked_candidates:
                self.apply_prediction_quality_curve(candidate)
                if self.execution_policy is not None:
                    self.execution_policy.apply_ranked_candidate(candidate)
            mark_stage("quality_policy_sec")
            paper_consensus_policy = self.paper_consensus_policy()
            if paper_consensus_policy["enabled"]:
                for candidate in ranked_candidates:
                    candidate["paper_consensus_policy"] = paper_consensus_policy
            ranked = self.consolidate_ranked_signals(ranked_candidates)
            mark_stage("consolidation_sec")
            for row in ranked:
                recent_evidence = self.performance.recent_negative_veto(
                    str(row.get("lane_id") or ""),
                    min_samples=int(
                        getattr(self.args, "execution_recent_veto_min_samples", 5)
                    ),
                    max_win_rate=safe_float(
                        getattr(self.args, "execution_recent_veto_max_win_rate", 40.0),
                        40.0,
                    ),
                )
                row["recent_lane_evidence"] = recent_evidence
                prediction_quality = row.get("prediction_quality") or {}
                validation = self.candidate_validation_state(
                    row,
                    recent_evidence,
                    prediction_quality,
                    min_historical_reliability=safe_float(
                        getattr(
                            self.args,
                            "execution_min_historical_reliability",
                            0.15,
                        ),
                        0.15,
                    ),
                )
                row["execution_validation"] = validation
                if recent_evidence["veto"]:
                    blocked_by = list(row.get("signal_blocked_by") or [])
                    if "recent_negative_lane" not in blocked_by:
                        blocked_by.append("recent_negative_lane")
                    row["signal_blocked_by"] = blocked_by
                    row["signal_eligible"] = False
                if validation["negative_historical_warmup"]:
                    blocked_by = list(row.get("signal_blocked_by") or [])
                    if "negative_historical_warmup" not in blocked_by:
                        blocked_by.append("negative_historical_warmup")
                    row["signal_blocked_by"] = blocked_by
                    row["signal_eligible"] = False
                if (
                    not bool(
                        getattr(
                            self.args,
                            "execution_allow_unvalidated_signals",
                            True,
                        )
                    )
                    and not validation["validated"]
                ):
                    blocked_by = list(row.get("signal_blocked_by") or [])
                    if "unvalidated_signal" not in blocked_by:
                        blocked_by.append("unvalidated_signal")
                    row["signal_blocked_by"] = blocked_by
                    row["signal_eligible"] = False
                market_target = execution_horizon_market_target(row)
                row["market_session_target"] = market_target
                if market_target.get("market_open") is False:
                    blocked_by = list(row.get("signal_blocked_by") or [])
                    if "target_market_closed" not in blocked_by:
                        blocked_by.append("target_market_closed")
                    row["signal_blocked_by"] = blocked_by
                    row["signal_eligible"] = False
            mark_stage("validation_sec")
            ranked.sort(key=self._signal_rank_key, reverse=True)
            eligible = [row for row in ranked if row.get("signal_eligible")]
            self.last_qualified_candidates = eligible
            top = []
            top_fields = (
                "id", "lane_id", "family", "profile", "instrument", "direction",
                "direction_state",
                "execution_horizon_sec", "signal_confidence", "signal_score",
                "normalized_rank_score", "expected_margin_return_per_hour_pct",
                "confidence_adjusted_margin_return_per_hour_pct",
                "liquidity_quality", "liquidity_prior", "spread_pips", "margin_rate",
                "instant_projected_net_pips", "projected_net_pips",
                "historical_expected_net_pips", "historical_reliability",
                "prediction_quality", "uncalibrated_signal_confidence",
                "uncalibrated_projected_net_pips",
                "recent_lane_evidence", "execution_validation",
                "agreement_family_count", "opposing_family_count",
                "agreement_archetype_count", "opposing_archetype_count",
                "conflicted_archetype_count", "agreement_archetype_probability",
                "agreement_archetypes", "opposing_archetypes", "conflicted_archetypes",
                "strategy_archetype", "strategy_independence_shadow_only",
                "signal_eligible", "signal_blocked_by",
                "preferred_horizon_sec", "projected_net_pips_per_hour",
                "component_count", "component_family_count", "component_families",
                "component_archetype_count", "component_archetypes",
                "component_models", "component_timeframes", "component_horizons_sec",
                "entry_exit_component_count", "direction_conflict", "opposing_direction",
                "opposing_signal_confidence", "signal_group_id",
                "aggregate_signal_id", "aggregate_signal_lineage_contract_id",
                "direction_conflict_contract_id",
                "direction_conflict_contributor_count",
                "direction_conflict_account_eligible_count",
                "direction_conflict_execution_component_count",
                "direction_conflict_shadow_only_count",
                "direction_conflict_only_shadow_or_account_ineligible",
                "direction_conflict_contributors",
                "paper_consensus_eligible", "paper_consensus_evidence",
                "sma_filter", "sma_filter_point", "sma_filter_weight",
                "market_session_target",
            )
            horizon_fields = (
                "horizon_sec", "direction", "direction_state",
                "signal_eligible", "signal_blocked_by",
                "signal_confidence", "probability_up", "signal_score",
                "raw_probability_up", "filtered_probability_up",
                "execution_probability_up",
                "normalized_rank_score", "projected_net_pips",
                "projected_net_pips_per_hour", "ensemble_signed_net_pips",
                "raw_ensemble_signed_net_pips",
                "execution_ensemble_signed_net_pips",
                "instant_projected_net_pips", "projected_gross_movement_pips",
                "projection_method", "directional_probability_edge",
                "signal_reference_horizon_sec", "volatility_projection_cap_pips",
                "ensemble_direction_score", "ensemble_aligned_weight_pct",
                "raw_ensemble_aligned_weight_pct",
                "gross_to_spread", "all_contributor_gross_to_spread",
                "directional_gross_to_spread", "minimum_gross_to_spread",
                "execution_uncertainty_cost_pips",
                "effective_min_signal_confidence",
                "effective_min_expected_net_pips", "component_count",
                "research_only_component_count", "filtered_surface_available",
                "filter_passed_component_count", "execution_component_count",
                "eligible_component_count", "family_count", "archetype_count",
                "agreement_archetype_count", "opposing_archetype_count",
                "conflicted_archetype_count", "archetype_independence_ratio",
                "archetype_aligned_weight_pct", "strategy_independence_shadow_only",
                "archetypes", "strategy_independence", "timeframe_count",
                "best_family", "best_model_id", "best_input_timeframe",
                "setup_class_counts", "source_kind_counts",
                "model_gap_component_count", "aggregation_version",
                "paper_consensus_component_count", "paper_consensus_eligible",
                "paper_consensus_evidence",
                "aggregate_signal_id", "aggregate_signal_lineage_contract_id",
                "direction_conflict", "direction_conflict_contract_id",
                "direction_conflict_contributor_count",
                "direction_conflict_account_eligible_count",
                "direction_conflict_execution_component_count",
                "direction_conflict_shadow_only_count",
                "direction_conflict_only_shadow_or_account_ineligible",
                "direction_conflict_contributors",
                "sma_filter", "sma_filter_point", "sma_filter_weight",
            )
            snapshot_limit = max(
                self.args.execution_top_lanes,
                int(
                    safe_float(
                        getattr(
                            self.args,
                            "execution_snapshot_top_signals",
                            self.args.execution_top_lanes,
                        ),
                        self.args.execution_top_lanes,
                    )
                ),
            )
            for row in ranked[:snapshot_limit]:
                compact = {key: row.get(key) for key in top_fields}
                compact["horizon_breakdown"] = [
                    {key: point.get(key) for key in horizon_fields}
                    for point in row.get("horizon_breakdown") or []
                ]
                top.append(compact)
            mark_stage("snapshot_compaction_sec")
            rank_timings["total_sec"] = round(
                time.perf_counter() - rank_started,
                6,
            )
            self.last_rank_timings = rank_timings
            return (eligible[0] if eligible else None), top
        observed = self.performance.top(self.args.execution_min_samples, max(self.args.execution_top_lanes, 32))
        top = [
            row
            for row in observed
            if safe_float(row.get("avg")) >= self.args.execution_min_average_pips
            and safe_float(row.get("median")) >= self.args.execution_min_median_pips
            and safe_float(row.get("win_rate")) >= self.args.execution_min_win_rate
            and safe_float(row.get("lower_confidence"), -math.inf) >= self.args.execution_min_lower_confidence_pips
        ][: self.args.execution_top_lanes]
        by_lane = {row["lane_id"]: row for row in top}
        eligible = [candidate for candidate in candidate_pool if candidate["lane_id"] in by_lane]
        eligible.sort(
            key=lambda candidate: (
                by_lane[candidate["lane_id"]]["score"],
                safe_float(candidate.get("signal_to_spread")),
                -safe_float(candidate.get("spread_pips")),
            ),
            reverse=True,
        )
        self.last_qualified_candidates = eligible
        mark_stage("fallback_rank_sec")
        rank_timings["total_sec"] = round(
            time.perf_counter() - rank_started,
            6,
        )
        self.last_rank_timings = rank_timings
        return (eligible[0] if eligible else None), top

    @staticmethod
    def hot_feed_publish_candidates(
        candidates: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Omit non-actionable hard rejects from the latency-sensitive feed.

        Hard rejects remain in the local shadow log and outcome diagnostics.
        They have already failed a non-negotiable setup, spread, or quote gate,
        so copying them into the inter-process candidate WAL cannot improve an
        account decision and can delay the viable rows behind them.
        """

        return [
            row
            for row in candidates
            if str(row.get("preconsensus_class") or "accepted")
            != "hard_reject"
        ]

    def acquire_execution_lock(self, timeout_sec: float = 3.0) -> int | None:
        path = self.args.execution_lock_path
        path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + max(0.1, timeout_sec)
        while time.monotonic() < deadline:
            try:
                descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(descriptor, f"{os.getpid()} {time.time()}\n".encode("ascii"))
                return descriptor
            except FileExistsError:
                try:
                    if time.time() - path.stat().st_mtime > 30.0:
                        path.unlink()
                        continue
                except OSError:
                    pass
                time.sleep(0.05)
        return None

    def release_execution_lock(self, descriptor: int) -> None:
        try:
            os.close(descriptor)
        finally:
            try:
                self.args.execution_lock_path.unlink()
            except OSError:
                pass

    def submit_selected_locked(self, selected: dict[str, Any]) -> None:
        if (
            self.signal_feed is not None
            and self.signal_feed.seconds_since_last_fill() < self.args.execution_cooldown_sec
        ):
            log_line(
                self.log_path,
                "execution_skipped",
                reason="shared_cooldown_under_lock",
                selected_id=selected.get("id"),
            )
            return
        trades = self.open_trades()
        blocker = self.portfolio_blocker(selected, trades)
        if blocker:
            log_line(
                self.log_path,
                "execution_skipped",
                reason=blocker,
                open_trade_count=len(trades),
                selected_id=selected.get("id"),
                instrument=selected.get("instrument"),
            )
            return
        reentry_reason, reentry_detail = self.reentry_blocker(selected, trades)
        if reentry_reason:
            log_line(
                self.log_path,
                "execution_skipped",
                reason=reentry_reason,
                selected_id=selected.get("id"),
                instrument=selected.get("instrument"),
                direction=selected.get("direction"),
                reentry_guard=reentry_detail,
            )
            return
        cost_reason, cost_detail = self.cost_capture_blocker(selected)
        if cost_reason:
            log_line(
                self.log_path,
                "execution_skipped",
                reason=cost_reason,
                selected_id=selected.get("id"),
                instrument=selected.get("instrument"),
                direction=selected.get("direction"),
                cost_capture_gate=cost_detail,
            )
            return
        selected = dict(selected)
        exit_source = "profile_default"
        exit_recommendation: dict[str, Any] | None = None
        if self.args.execution_use_fitted_exits and self.exit_fit is not None:
            selected_regime = str(
                selected.get("volatility_regime")
                or classify_volatility_regime(
                    selected.get("atr_pips"),
                    selected.get("spread_pips"),
                )
            )
            exit_recommendation = self.exit_fit.recommendation(
                str(selected["lane_id"]),
                str(selected["family"]),
                str(selected.get("instrument") or ""),
                int(safe_float(selected.get("execution_horizon_sec"))),
                selected_regime,
            )
            if exit_recommendation is not None:
                selected["stop_loss_pips"] = safe_float(exit_recommendation.get("stop_pips"))
                selected["take_profit_r"] = safe_float(exit_recommendation.get("target_r"))
                exit_source = (
                    "chronological_holdout_fit:"
                    + str(exit_recommendation.get("scope") or "global")
                )

        frozen_exit_policy: dict[str, Any] = {}
        if self.execution_policy is not None:
            frozen_exit_policy = self.execution_policy.exit_policy(
                family=str(selected.get("family") or ""),
                horizon_sec=int(safe_float(selected.get("execution_horizon_sec"))),
            )
        policy_shape = (
            frozen_exit_policy.get("policy")
            if isinstance(frozen_exit_policy.get("policy"), dict)
            else frozen_exit_policy
        )
        if isinstance(policy_shape, dict) and policy_shape:
            policy_kind = str(policy_shape.get("kind") or "")
            if policy_shape.get("stop_pips") is not None:
                selected["stop_loss_pips"] = safe_float(policy_shape.get("stop_pips"))
            if policy_kind == "fixed" and policy_shape.get("target_pips") is not None:
                stop_value = max(0.1, safe_float(selected.get("stop_loss_pips"), 5.0))
                selected["take_profit_r"] = safe_float(policy_shape.get("target_pips")) / stop_value
                selected["exit_open_ended_override"] = False
            elif policy_kind == "trailing":
                selected["fitted_trailing_activation_pips"] = safe_float(
                    policy_shape.get("activation_pips")
                )
                selected["fitted_trailing_distance_pips"] = safe_float(
                    policy_shape.get("trail_distance_pips")
                )
                selected["fitted_trailing_spread_multiple"] = safe_float(
                    policy_shape.get("spread_multiple")
                )
                selected["exit_open_ended_override"] = True
            elif policy_kind == "time_stop":
                selected["execution_exit_horizon_sec"] = max(
                    10,
                    int(
                        safe_float(selected.get("execution_horizon_sec"))
                        * max(
                            0.05,
                            min(1.0, safe_float(policy_shape.get("horizon_fraction"), 1.0)),
                        )
                    ),
                )
                selected["exit_open_ended_override"] = True
            selected["frozen_exit_policy"] = frozen_exit_policy
            exit_source = "frozen_nested_walkforward:" + str(
                frozen_exit_policy.get("scope") or "overall"
            )

        horizon_risk_shape = self.apply_horizon_risk_shape(selected)
        stop_pips = max(0.1, safe_float(selected.get("stop_loss_pips"), 5.0))
        spread_pips = max(0.0, safe_float(selected.get("spread_pips")))
        confidence = max(0.5, safe_float(selected.get("signal_confidence"), 0.5))
        instrument_meta = self.instrument_meta.get(str(selected.get("instrument") or "")) or {}
        pip = max(1e-12, safe_float(selected.get("pip")))
        broker_minimum_trailing_pips = (
            safe_float(instrument_meta.get("minimum_trailing_stop_distance")) / pip
        )
        fitted_trail_distance = safe_float(selected.get("fitted_trailing_distance_pips"))
        if fitted_trail_distance > 0.0:
            fitted_spread_multiple = max(
                0.0,
                safe_float(selected.get("fitted_trailing_spread_multiple")),
            )
            trail_distance = max(
                fitted_trail_distance,
                spread_pips * fitted_spread_multiple,
                broker_minimum_trailing_pips,
            )
            trail_activation = max(
                safe_float(selected.get("fitted_trailing_activation_pips")),
                trail_distance,
            )
        else:
            trail_distance = max(
                safe_float(getattr(self.args, "execution_min_trailing_pips", 2.5), 2.5),
                spread_pips * safe_float(getattr(self.args, "execution_trailing_spread_multiple", 2.5), 2.5),
                stop_pips * (
                    safe_float(getattr(self.args, "execution_trailing_stop_r", 0.55), 0.55)
                    + 0.20 * max(0.0, min(1.0, (confidence - 0.54) / 0.21))
                ),
                broker_minimum_trailing_pips,
            )
            trail_activation = max(
                trail_distance
                + safe_float(
                    getattr(self.args, "execution_trailing_activation_spread_multiple", 0.5),
                    0.5,
                )
                * spread_pips,
                stop_pips * safe_float(getattr(self.args, "execution_trailing_activation_r", 0.9), 0.9),
            )
        selected["trailing_distance_pips"] = round(trail_distance, 3)
        selected["trailing_activation_pips"] = round(trail_activation, 3)
        if horizon_risk_shape.get("applied"):
            selected["trailing_distance_pips"] = round(
                max(
                    safe_float(selected["trailing_distance_pips"]),
                    0.65 * stop_pips,
                ),
                3,
            )
            selected["trailing_activation_pips"] = round(
                max(
                    safe_float(selected["trailing_activation_pips"]),
                    0.90 * stop_pips,
                    safe_float(selected["trailing_distance_pips"]),
                ),
                3,
            )
        selected["open_ended_profit"] = bool(
            selected.get(
                "exit_open_ended_override",
                getattr(self.args, "execution_open_ended_profit", False),
            )
        )

        summary = self.client.get(f"/v3/accounts/{self.account_id}/summary", params={}).get("account") or {}
        units, sizing = self.dynamic_sizing(selected, summary)
        if units <= 0:
            log_line(
                self.log_path,
                "execution_skipped",
                reason="sizing_below_minimum",
                selected_id=selected.get("id"),
                sizing=sizing,
            )
            return
        margin_blocker, margin_exposure = self.portfolio_margin_blocker(
            selected,
            trades,
            summary,
            sizing,
        )
        if margin_blocker:
            log_line(
                self.log_path,
                "execution_skipped",
                reason=margin_blocker,
                selected_id=selected.get("id"),
                instrument=selected.get("instrument"),
                margin_exposure=margin_exposure,
            )
            return

        client_id = f"lab-{datetime.now(timezone.utc):%Y%m%d%H%M%S}-{secrets.token_hex(3)}"
        body = build_practice_order(
            selected,
            units,
            self.args.execution_max_slippage_pips,
            client_id,
        )
        rank = selected.get("promotion_evidence") or self.performance.stats(str(selected["lane_id"]))
        forecast_to_submit_ms = (
            round((time.monotonic() - safe_float(selected.get("forecast_created_monotonic"))) * 1000.0, 3)
            if selected.get("forecast_created_monotonic") is not None
            else None
        )
        log_line(
            self.log_path,
            "execution_selected",
            selected_id=selected["id"],
            lane_id=selected["lane_id"],
            family=selected["family"],
            profile=selected["profile"],
            instrument=selected["instrument"],
            direction=selected["direction"],
            execution_horizon_sec=selected.get("execution_horizon_sec"),
            execution_exit_horizon_sec=selected.get("execution_exit_horizon_sec"),
            execution_policy_id=(
                ""
                if self.execution_policy is None
                else self.execution_policy.policy_id
            ),
            frozen_exit_policy=selected.get("frozen_exit_policy") or {},
            projected_net_pips=selected.get("projected_net_pips"),
            instant_projected_net_pips=selected.get("instant_projected_net_pips"),
            signal_confidence=selected.get("signal_confidence"),
            signal_score=selected.get("signal_score"),
            historical_expected_net_pips=selected.get("historical_expected_net_pips"),
            historical_reliability=selected.get("historical_reliability"),
            agreement_family_count=selected.get("agreement_family_count"),
            opposing_family_count=selected.get("opposing_family_count"),
            agreement_archetype_count=selected.get("agreement_archetype_count"),
            opposing_archetype_count=selected.get("opposing_archetype_count"),
            conflicted_archetype_count=selected.get("conflicted_archetype_count"),
            strategy_independence_shadow_only=selected.get(
                "strategy_independence_shadow_only"
            ),
            promotion_segments=selected.get("promotion_segments") or {},
            rank=rank,
            units=units,
            sizing=sizing,
            margin_exposure=margin_exposure,
            exit_source=exit_source,
            exit_recommendation=exit_recommendation or {},
            open_ended_profit=selected.get("open_ended_profit"),
            trailing_activation_pips=selected.get("trailing_activation_pips"),
            trailing_distance_pips=selected.get("trailing_distance_pips"),
            horizon_risk_shape=horizon_risk_shape,
            order=body["order"],
            forecast_to_submit_ms=forecast_to_submit_ms,
        )
        reference_price = (
            safe_float(selected.get("ask"))
            if str(selected.get("direction")) == "buy"
            else safe_float(selected.get("bid"))
        )
        price_bound = safe_float((body.get("order") or {}).get("priceBound"))
        quote_time = parse_rfc3339(str(selected.get("entry_time") or ""))
        quote_age_sec = (
            None
            if quote_time is None
            else max(0.0, (datetime.now(timezone.utc) - quote_time).total_seconds())
        )
        # Check current authorization after all preparation and logging, with
        # no intervening disk or broker reads before the order write.
        final_blocker = self.final_submission_blocker(selected, units, body)
        if final_blocker:
            log_line(
                self.log_path,
                "execution_skipped",
                reason=final_blocker,
                selected_id=selected.get("id"),
                instrument=selected.get("instrument"),
                direction=selected.get("direction"),
                units=units,
            )
            return
        # Keep a short attempt backoff even when the broker rejects or cancels
        # the order; the persistent post-exit re-entry policy is separate.
        self.last_entry_monotonic = time.monotonic()
        order_started = time.perf_counter()
        try:
            payload = self.client.write(
                "POST",
                f"/v3/accounts/{self.account_id}/orders",
                body,
                client_request_id=client_id,
            )
        except OandaApiError as exc:
            order_roundtrip_ms = round(
                (time.perf_counter() - order_started) * 1000.0,
                3,
            )
            telemetry = self._record_order_telemetry(
                {
                    "status": "error",
                    "selected_id": selected.get("id"),
                    "instrument": selected.get("instrument"),
                    "direction": selected.get("direction"),
                    "order_roundtrip_ms": order_roundtrip_ms,
                    "forecast_to_submit_ms": forecast_to_submit_ms,
                    "quote_age_sec": quote_age_sec,
                    "reference_entry_price": reference_price,
                    "price_bound": price_bound,
                    "error_status": exc.status,
                    "outcome_uncertain": exc.outcome_uncertain,
                }
            )
            if self.signal_feed is not None:
                execution_row = dict(selected)
                execution_row["execution_telemetry"] = telemetry
                self.signal_feed.record_execution(client_id, execution_row, "error")
            if exc.status in {401, 403} or exc.outcome_uncertain:
                self.disabled_reason = "authorization" if exc.status in {401, 403} else "uncertain_order"
            log_line(
                self.log_path,
                "practice_order_error",
                selected_id=selected["id"],
                status=exc.status,
                request_id=exc.request_id,
                outcome_uncertain=exc.outcome_uncertain,
                execution_disabled=bool(self.disabled_reason),
                order_roundtrip_ms=order_roundtrip_ms,
                execution_telemetry=telemetry,
                error=str(exc)[:500],
            )
            return
        fill = payload.get("orderFillTransaction") or {}
        opened = fill.get("tradeOpened") or {}
        trade_id = str(opened.get("tradeID") or "")
        if not trade_id:
            order_roundtrip_ms = round(
                (time.perf_counter() - order_started) * 1000.0,
                3,
            )
            current_reference = None
            if self.price_snapshot_provider is not None:
                try:
                    current_quote = self.price_snapshot_provider().get(
                        str(selected.get("instrument") or "")
                    )
                    if current_quote is not None:
                        current_reference = (
                            safe_float(current_quote.ask)
                            if str(selected.get("direction")) == "buy"
                            else safe_float(current_quote.bid)
                        )
                except Exception:
                    current_reference = None
            missed_entry_move_pips = None
            beyond_bound_pips = None
            if current_reference is not None and pip > 0.0:
                missed_entry_move_pips = (
                    (current_reference - reference_price) / pip
                    if str(selected.get("direction")) == "buy"
                    else (reference_price - current_reference) / pip
                )
                if price_bound > 0.0:
                    beyond_bound_pips = max(
                        0.0,
                        (current_reference - price_bound) / pip
                        if str(selected.get("direction")) == "buy"
                        else (price_bound - current_reference) / pip,
                    )
            reject = payload.get("orderRejectTransaction") or {}
            cancel = payload.get("orderCancelTransaction") or {}
            telemetry = self._record_order_telemetry(
                {
                    "status": "not_filled",
                    "selected_id": selected.get("id"),
                    "instrument": selected.get("instrument"),
                    "direction": selected.get("direction"),
                    "order_roundtrip_ms": order_roundtrip_ms,
                    "forecast_to_submit_ms": forecast_to_submit_ms,
                    "quote_age_sec": quote_age_sec,
                    "reference_entry_price": reference_price,
                    "current_entry_price": current_reference,
                    "price_bound": price_bound,
                    "missed_entry_move_pips": (
                        None
                        if missed_entry_move_pips is None
                        else round(missed_entry_move_pips, 4)
                    ),
                    "beyond_bound_pips": (
                        None
                        if beyond_bound_pips is None
                        else round(beyond_bound_pips, 4)
                    ),
                    "reason": str(
                        cancel.get("reason") or reject.get("rejectReason") or ""
                    ),
                }
            )
            if self.signal_feed is not None:
                execution_row = dict(selected)
                execution_row["execution_telemetry"] = telemetry
                self.signal_feed.record_execution(
                    client_id,
                    execution_row,
                    "not_filled",
                )
            log_line(
                self.log_path,
                "practice_order_not_filled",
                selected_id=selected["id"],
                request_id=payload.get("_requestID"),
                reject=reject,
                cancel=cancel,
                order_roundtrip_ms=order_roundtrip_ms,
                execution_telemetry=telemetry,
            )
            return
        self.fills += 1
        self.last_entry_monotonic = time.monotonic()
        self.has_owned_trade = True
        order_roundtrip_ms = round(
            (time.perf_counter() - order_started) * 1000.0,
            3,
        )
        fill_price = safe_float(fill.get("price"))
        adverse_slippage_pips = (
            (fill_price - reference_price) / pip
            if str(selected.get("direction")) == "buy"
            else (reference_price - fill_price) / pip
        )
        telemetry = self._record_order_telemetry(
            {
                "status": "filled",
                "selected_id": selected.get("id"),
                "trade_id": trade_id,
                "instrument": selected.get("instrument"),
                "direction": selected.get("direction"),
                "order_roundtrip_ms": order_roundtrip_ms,
                "forecast_to_submit_ms": forecast_to_submit_ms,
                "quote_age_sec": quote_age_sec,
                "reference_entry_price": reference_price,
                "fill_price": fill_price,
                "price_bound": price_bound,
                "adverse_slippage_pips": round(adverse_slippage_pips, 4),
            }
        )
        self.owned_trade_state[trade_id] = {
            "instrument": str(selected.get("instrument") or ""),
            "direction": str(selected.get("direction") or ""),
            "factor": self.jpy_factor_key(
                str(selected.get("instrument") or ""),
                str(selected.get("direction") or ""),
            ),
            "open_time": utc_now(),
        }
        if self.signal_feed is not None:
            execution_row = dict(selected)
            execution_row["execution_telemetry"] = telemetry
            self.signal_feed.record_execution(
                client_id,
                execution_row,
                "filled",
                trade_id,
            )
        log_line(
            self.log_path,
            "practice_order_filled",
            selected_id=selected["id"],
            trade_id=trade_id,
            instrument=selected["instrument"],
            direction=selected["direction"],
            units=safe_float(opened.get("units")),
            signal_confidence=selected.get("signal_confidence"),
            signal_score=selected.get("signal_score"),
            projected_net_pips=selected.get("projected_net_pips"),
            sizing=sizing,
            price=fill_price,
            half_spread_cost=safe_float(fill.get("halfSpreadCost")),
            order_roundtrip_ms=order_roundtrip_ms,
            adverse_slippage_pips=round(adverse_slippage_pips, 4),
            execution_telemetry=telemetry,
            request_id=payload.get("_requestID"),
        )

    def write_signal_snapshot(
        self,
        selected: dict[str, Any] | None,
        top: list[dict[str, Any]],
        raw_candidate_count: int,
        feed_candidate_count: int,
    ) -> None:
        path = getattr(self.args, "execution_signal_snapshot", None)
        now = time.monotonic()
        default_snapshot_interval_sec = (
            5.0
            if bool(getattr(self.args, "execution_feed_consumer_only", False))
            else 0.5
        )
        snapshot_interval_sec = max(
            0.5,
            safe_float(
                getattr(self.args, "execution_signal_snapshot_sec", None),
                default_snapshot_interval_sec,
            ),
        )
        if (
            path is None
            or now - self.last_signal_snapshot_monotonic
            < snapshot_interval_sec
        ):
            return
        snapshot_started = time.perf_counter()
        coverage_started = snapshot_started
        coverage_refreshed = False
        coverage_skipped_consumer_only = bool(
            getattr(self.args, "execution_feed_consumer_only", False)
        )
        if (
            not coverage_skipped_consumer_only
            and
            self.signal_feed is not None
            and now - self.last_feed_coverage_monotonic >= 10.0
        ):
            try:
                self.feed_coverage_cache = self.signal_feed.coverage(
                    max(
                        60.0,
                        safe_float(
                            getattr(
                                self.args,
                                "execution_signal_feed_ttl_sec",
                                8.0,
                            ),
                            8.0,
                        )
                        * 4.0,
                    )
                )
                self.last_feed_coverage_monotonic = now
                coverage_refreshed = True
            except (OSError, sqlite3.Error):
                pass
        after_coverage = time.perf_counter()
        top_instruments = sorted(
            {
                str(row.get("instrument") or "")
                for row in top
                if row.get("instrument")
            }
        )
        market_quotes: dict[str, dict[str, Any]] = {}
        market_quote_rejections: dict[str, str] = {}
        market_quote_provider_status = "unavailable"
        if self.price_snapshot_provider is not None:
            try:
                prices = self.price_snapshot_provider()
                market_quote_provider_status = "ok"
                for instrument in top_instruments:
                    quote = prices.get(instrument)
                    rejection_reason = outcome_quote_rejection_reason(quote)
                    if rejection_reason:
                        market_quote_rejections[instrument] = rejection_reason
                        continue
                    market_quotes[instrument] = {
                        "bid": safe_float(quote.bid),
                        "ask": safe_float(quote.ask),
                        "time": str(quote.time or ""),
                        "source": str(
                            getattr(quote, "source", "") or "strategy_price_stream"
                        ),
                        "tradeable": bool(
                            getattr(quote, "tradeable", True)
                        ),
                    }
            except Exception as exc:
                market_quote_provider_status = "error"
                log_line(
                    self.log_path,
                    "execution_signal_snapshot_quote_error",
                    error=f"{type(exc).__name__}: {exc}"[:300],
                )
        if market_quote_provider_status != "ok":
            market_quote_rejections = {
                instrument: f"quote_provider_{market_quote_provider_status}"
                for instrument in top_instruments
            }
        market_quote_rejection_counts = dict(
            sorted(Counter(market_quote_rejections.values()).items())
        )
        after_quotes = time.perf_counter()
        feed_expected = int(self.feed_coverage_cache.get("expected_contributors") or 0)
        feed_fresh = int(self.feed_coverage_cache.get("fresh_contributors") or 0)
        compact_top = self.compact_signal_rows(top)
        after_compaction = time.perf_counter()
        selected_direction_conflict = bool(
            selected is not None and selected.get("direction_conflict")
        )
        selected_stage = (
            "none"
            if selected is None
            else "signal_gate_pre_final_execution_gates"
        )
        selected_final_gate_status = (
            "not_selected"
            if selected is None
            else "blocked_direction_conflict"
            if selected_direction_conflict
            else "not_yet_evaluated"
        )
        payload = {
            "schema_version": 3,
            "updated_at": utc_now(),
            "account_suffix": self.account_id[-4:],
            "producer": str(getattr(self.args, "execution_feed_source", "strategy_lab")),
            "selection_mode": "consolidated_instrument_horizon_curve",
            "raw_candidate_count": int(raw_candidate_count),
            "feed_candidate_count": int(feed_candidate_count),
            "consolidated_signal_count": len(top),
            "qualified_signal_count": len(self.last_qualified_candidates),
            # ``qualified_signal_count`` is the signal-level gate and is kept
            # for backward-compatible research diagnostics.  Direction-
            # conflicted candidates are intentionally shadow-only at the
            # final practice-execution gate, so expose the honest subset
            # separately instead of making the dashboard imply they can fill.
            "nonconflicting_qualified_signal_count": sum(
                1
                for row in self.last_qualified_candidates
                if not bool(row.get("direction_conflict"))
            ),
            "contribution_feed": self.feed_coverage_cache,
            "snapshot_quality": {
                "local_candidate_count": int(raw_candidate_count),
                "merged_feed_candidate_count": int(feed_candidate_count),
                "fresh_contributors": feed_fresh,
                "expected_contributors": feed_expected,
                "contributor_coverage_ratio": (
                    feed_fresh / feed_expected if feed_expected > 0 else None
                ),
                "market_quote_scope": "ranked_signal_instruments",
                "market_quote_provider_status": market_quote_provider_status,
                "market_quote_expected_instrument_count": len(top_instruments),
                "market_quote_count": len(market_quotes),
                "market_quote_coverage_ratio": (
                    len(market_quotes) / len(top_instruments)
                    if top_instruments
                    else None
                ),
                "market_quote_rejected_count": len(market_quote_rejections),
                "market_quote_rejection_reasons": market_quote_rejection_counts,
                "market_quote_rejected_instruments": market_quote_rejections,
            },
            "market_quotes": market_quotes,
            # ``selected`` is chosen before portfolio, re-entry, cost-capture,
            # timing, cooldown, quote-age, lifecycle, and authorization gates.
            # Keep the legacy object for consumers, but make its stage explicit
            # so a signal-level selection cannot be mistaken for a routable or
            # authorized Practice-007 order candidate.
            "selected_stage": selected_stage,
            "selected_final_gate_status": selected_final_gate_status,
            "selected_routable_after_direction_conflict_gate": bool(
                selected is not None and not selected_direction_conflict
            ),
            "selected": (
                None
                if selected is None
                else {
                    "id": selected.get("id"),
                    "signal_group_id": selected.get("signal_group_id"),
                    "instrument": selected.get("instrument"),
                    "direction": selected.get("direction"),
                    "preferred_horizon_sec": selected.get("preferred_horizon_sec"),
                    "signal_confidence": selected.get("signal_confidence"),
                    "projected_net_pips": selected.get("projected_net_pips"),
                    "selection_stage": selected_stage,
                    "direction_conflict": selected_direction_conflict,
                    "routable_after_direction_conflict_gate": bool(
                        not selected_direction_conflict
                    ),
                    "final_execution_status": selected_final_gate_status,
                }
            ),
            "top_signals": compact_top,
        }
        async_write = self.signal_snapshot_writer is not None
        try:
            if self.signal_snapshot_writer is not None:
                self.last_signal_snapshot_writer_stats = (
                    self.signal_snapshot_writer.submit(payload)
                )
            else:
                atomic_json(Path(path), payload)
            self.last_signal_snapshot_monotonic = now
        except OSError as exc:
            log_line(self.log_path, "execution_signal_snapshot_error", error=str(exc)[:300])
        after_write = time.perf_counter()
        if self.signal_snapshot_writer is not None:
            self.last_signal_snapshot_writer_stats = (
                self.signal_snapshot_writer.status()
            )
        self.last_signal_snapshot_timings = {
            "coverage_refreshed": coverage_refreshed,
            "coverage_skipped_consumer_only": (
                coverage_skipped_consumer_only
            ),
            "coverage_ms": round(
                (after_coverage - coverage_started) * 1000.0,
                3,
            ),
            "quote_copy_ms": round(
                (after_quotes - after_coverage) * 1000.0,
                3,
            ),
            "compaction_ms": round(
                (after_compaction - after_quotes) * 1000.0,
                3,
            ),
            "atomic_write_ms": (
                0.0
                if async_write
                else round((after_write - after_compaction) * 1000.0, 3)
            ),
            "queue_ms": (
                round((after_write - after_compaction) * 1000.0, 3)
                if async_write
                else 0.0
            ),
            "async_writer": self.last_signal_snapshot_writer_stats,
            "total_ms": round(
                (after_write - snapshot_started) * 1000.0,
                3,
            ),
        }

    @staticmethod
    def compact_signal_rows(
        rows: list[dict[str, Any]],
        *,
        full_detail_rows: int = 1,
    ) -> list[dict[str, Any]]:
        """Keep the top breakdown while bounding repetitive snapshot/log payloads."""

        horizon_fields = {
            "horizon_sec",
            "direction",
            "probability_up",
            "signal_confidence",
            "projected_net_pips",
            "projected_net_pips_per_hour",
            "signal_score",
            "signal_eligible",
            "signal_blocked_by",
            # The position ledger derives a truthful research direction from
            # the unfiltered surface.  Dropping these fields from every row
            # after the first made current snapshots look like legacy input,
            # which created noisy legacy diagnostic cohorts and prevented the
            # current measurement arm from observing most ranked signals.
            "raw_probability_up",
            "raw_ensemble_signed_net_pips",
            "raw_ensemble_aligned_weight_pct",
            "component_count",
            "eligible_component_count",
            "family_count",
            "archetype_count",
            "agreement_archetype_count",
            "opposing_archetype_count",
            "conflicted_archetype_count",
            "archetype_independence_ratio",
            "archetype_aligned_weight_pct",
            "strategy_independence_shadow_only",
            "timeframe_count",
            "aggregation_method",
            "ensemble_aligned_weight_pct",
            "setup_class_counts",
            "best_model_id",
            "best_input_timeframe",
            "best_family",
            "gross_to_spread",
            "all_contributor_gross_to_spread",
            "directional_gross_to_spread",
            "aggregate_signal_id",
            "aggregate_signal_lineage_contract_id",
            "direction_conflict",
            "direction_conflict_contract_id",
            "direction_conflict_contributor_count",
            "direction_conflict_account_eligible_count",
            "direction_conflict_execution_component_count",
            "direction_conflict_shadow_only_count",
            "direction_conflict_only_shadow_or_account_ineligible",
            "direction_conflict_contributors",
        }
        output: list[dict[str, Any]] = []
        for index, row in enumerate(rows):
            if index < max(0, int(full_detail_rows)):
                output.append(row)
                continue
            compact = dict(row)
            compact["horizon_breakdown"] = [
                {
                    name: value
                    for name, value in horizon.items()
                    if name in horizon_fields
                }
                for horizon in row.get("horizon_breakdown") or []
                if isinstance(horizon, dict)
            ]
            output.append(compact)
        return output

    @staticmethod
    def compact_execution_log_rows(
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Bound periodic executor logs without changing signal snapshots."""

        fields = {
            "id",
            "signal_group_id",
            "instrument",
            "direction",
            "direction_state",
            "direction_conflict",
            "direction_conflict_contract_id",
            "direction_conflict_contributor_count",
            "direction_conflict_account_eligible_count",
            "direction_conflict_execution_component_count",
            "direction_conflict_shadow_only_count",
            "direction_conflict_only_shadow_or_account_ineligible",
            "direction_conflict_contributors",
            "aggregate_signal_id",
            "aggregate_signal_lineage_contract_id",
            "family",
            "profile",
            "lane_id",
            "strategy_archetype",
            "execution_horizon_sec",
            "preferred_horizon_sec",
            "signal_confidence",
            "projected_net_pips",
            "projected_net_pips_per_hour",
            "signal_score",
            "normalized_rank_score",
            "historical_expected_net_pips",
            "historical_reliability",
            "instant_projected_net_pips",
            "execution_validation",
            "signal_eligible",
            "signal_blocked_by",
            "paper_consensus_eligible",
            "agreement_archetype_count",
            "agreement_archetype_probability",
            "agreement_archetypes",
            "opposing_archetype_count",
            "opposing_archetypes",
            "conflicted_archetype_count",
            "component_count",
            "component_family_count",
            "component_timeframes",
            "strategy_independence_shadow_only",
        }
        return [
            {
                name: value
                for name, value in row.items()
                if name in fields
            }
            for row in rows
        ]

    @staticmethod
    def compact_promotion_log_rows(
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Keep promotion diagnostics useful without copying full evidence."""

        scalar_fields = {
            "lane_id",
            "family",
            "profile",
            "horizon_sec",
            "eligible",
            "blocked_by",
            "sample_count",
            "holdout_blocks",
            "independent_blocks",
            "pair_count",
            "session_count",
            "score",
            "adjusted_lower_confidence",
        }
        metric_fields = {"n", "avg", "median", "win_rate", "lower_confidence"}
        output: list[dict[str, Any]] = []
        for row in rows:
            compact = {
                name: value for name, value in row.items() if name in scalar_fields
            }
            for split in ("training", "holdout"):
                metrics = row.get(split)
                if isinstance(metrics, dict):
                    compact[split] = {
                        name: value
                        for name, value in metrics.items()
                        if name in metric_fields
                    }
            output.append(compact)
        return output

    def publish_research_forecasts(
        self,
        candidates: list[dict[str, Any]],
        source_suffix: str,
    ) -> dict[str, Any]:
        if self.signal_feed is None or not candidates:
            return {
                "available": self.signal_feed is not None,
                "received": len(candidates),
                "accepted": 0,
                "rejected": 0,
            }
        source = (
            f"{getattr(self.args, 'execution_feed_source', 'strategy_lab')}."
            f"{source_suffix}"
        )
        try:
            result = self.signal_feed.publish_forecasts(
                candidates,
                source,
                max(
                    300.0,
                    safe_float(
                        getattr(
                            self.args,
                            "execution_signal_feed_ttl_sec",
                            90.0,
                        ),
                        90.0,
                    ),
                ),
                max_age_sec=90.0,
            )
            candidate_ids = result.pop("candidate_ids", [])
            result["candidate_id_count"] = len(candidate_ids)
        except Exception as exc:
            try:
                self.signal_feed.connection.rollback()
            except Exception:
                pass
            log_line(
                self.log_path,
                "research_forecast_publish_error",
                source=source,
                error=f"{type(exc).__name__}: {exc}"[:500],
            )
            return {
                "available": True,
                "received": len(candidates),
                "accepted": 0,
                "rejected": len(candidates),
                "error": type(exc).__name__,
            }
        return {"available": True, **result}

    def publish_partial_cycle_shadow(
        self,
        candidates: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Publish an incomplete generation for cadence measurement only.

        Rows retain their final candidate identities so the completed
        generation replaces them later in the same cycle. They are forced
        research-only, and the fast consumer excludes this source kind from
        ranking so incomplete consensus cannot affect an order decision.
        """

        if self.signal_feed is None:
            return {
                "available": False,
                "received": len(candidates),
                "published": 0,
            }
        rows: list[dict[str, Any]] = []
        for candidate in self.hot_feed_publish_candidates(candidates):
            row = dict(candidate)
            row["account_eligible"] = False
            row["research_only"] = True
            row["research_blocked_reason"] = (
                "partial_cycle_incomplete_consensus"
            )
            row["source_kind"] = "partial_cycle_shadow"
            row["strategy_independence_shadow_only"] = True
            blockers = list(row.get("preconsensus_blockers") or [])
            if "partial_cycle_incomplete_consensus" not in blockers:
                blockers.append("partial_cycle_incomplete_consensus")
            row["preconsensus_blockers"] = blockers
            rows.append(row)
        source = (
            f"{getattr(self.args, 'execution_feed_source', 'strategy_lab')}"
            "_partial_shadow"
        )
        try:
            maintenance = self.signal_feed.publish(
                rows,
                source,
                safe_float(
                    getattr(
                        self.args,
                        "execution_signal_feed_ttl_sec",
                        90.0,
                    ),
                    90.0,
                ),
            )
        except sqlite3.OperationalError as exc:
            self.signal_feed.connection.rollback()
            log_line(
                self.log_path,
                "partial_cycle_shadow_publish_error",
                source=source,
                error=f"{type(exc).__name__}: {exc}"[:500],
            )
            return {
                "available": True,
                "received": len(candidates),
                "published": 0,
                "error": type(exc).__name__,
            }
        return {
            "available": True,
            "received": len(candidates),
            "published": len(rows),
            "source": source,
            "maintenance": maintenance,
        }

    def maybe_execute(self, candidates: list[dict[str, Any]]) -> None:
        self.last_execution_selection_blocks = []
        selected, top = self.ranked_candidate(candidates)
        observed = (
            list((self.promotion.state.get("top_evidence") or [])[: self.args.execution_top_lanes])
            if self.promotion is not None
            else self.performance.top(self.args.execution_min_samples, self.args.execution_top_lanes)
        )
        now = time.monotonic()
        feed_candidate_count = self.last_feed_candidate_count
        self.write_signal_snapshot(selected, top, len(candidates), feed_candidate_count)
        if now - self.last_selection_log_monotonic >= 60.0:
            log_line(
                self.log_path,
                "execution_selection_summary",
                matrix_candidates=len(candidates),
                accepted_candidates=sum(
                    1
                    for row in candidates
                    if str(row.get("preconsensus_class") or "accepted")
                    == "accepted"
                    and bool(row.get("account_eligible", True))
                ),
                near_miss_candidates=sum(
                    1
                    for row in candidates
                    if row.get("preconsensus_class") == "near_threshold"
                ),
                hard_reject_candidates=sum(
                    1
                    for row in candidates
                    if row.get("preconsensus_class") == "hard_reject"
                ),
                qualified_candidates=len(self.last_qualified_candidates),
                nonconflicting_qualified_candidates=sum(
                    1
                    for row in self.last_qualified_candidates
                    if not bool(row.get("direction_conflict"))
                ),
                top_lanes=self.compact_execution_log_rows(top),
                observed_top_lanes=self.compact_promotion_log_rows(observed),
                selection_mode="consolidated_instrument_horizon_curve",
                signal_feed_candidates=feed_candidate_count,
                signal_gate={
                    "min_confidence": safe_float(getattr(self.args, "execution_min_signal_confidence", 0.54), 0.54),
                    "min_expected_net_pips": safe_float(
                        getattr(self.args, "execution_min_signal_expected_net_pips", 0.05),
                        0.05,
                    ),
                    "historical_role": "confidence calibration and persistent-negative veto",
                    "path_quality_role": (
                        "chronological executable-return calibration with MFE/MAE, "
                        "time-to-positive, and negative-evidence veto"
                    ),
                    "whole_lane_promotion_required": False,
                },
                promotion_gate={
                    "min_samples": self.args.execution_min_samples,
                    "min_average_pips": self.args.execution_min_average_pips,
                    "min_median_pips": self.args.execution_min_median_pips,
                    "min_win_rate": self.args.execution_min_win_rate,
                    "min_lower_confidence_pips": self.args.execution_min_lower_confidence_pips,
                    "min_independent_blocks": self.args.execution_min_independent_blocks,
                    "min_holdout_blocks": self.args.execution_min_holdout_blocks,
                    "min_pairs": self.args.execution_min_pairs,
                    "min_sessions": self.args.execution_min_sessions,
                },
                rank_latency=self.last_rank_timings,
                feed_cache=self.last_feed_cache_stats,
                selected_id=None if selected is None else selected["id"],
                selected_horizon_sec=None if selected is None else selected.get("execution_horizon_sec"),
                **selection_fields(candidates, feed_candidate_count,
                                   self.last_qualified_candidates, selected,
                                   self.disabled_reason),
            )
            self.last_selection_log_monotonic = now
        if not bool(getattr(self.args, "execute_top_signals", False)):
            return
        if selected is None or self.disabled_reason:
            return
        trades = self.manage_open_trades(force=True)
        selection_blocks: list[dict[str, Any]] = []
        selected = None
        for row in self.last_qualified_candidates:
            portfolio_reason = self.portfolio_blocker(row, trades)
            reentry_reason, reentry_detail = self.reentry_blocker(row, trades)
            cost_reason, cost_detail = self.cost_capture_blocker(row)
            reason = portfolio_reason or reentry_reason or cost_reason
            if reason:
                selection_blocks.append(
                    {
                        "id": row.get("id"),
                        "instrument": row.get("instrument"),
                        "direction": row.get("direction"),
                        "reason": reason,
                        "detail": (
                            reentry_detail
                            if reentry_reason
                            else cost_detail
                            if cost_reason
                            else {}
                        ),
                    }
                )
                continue
            selected = row
            break
        self.last_execution_selection_blocks = selection_blocks[:12]
        if selected is None:
            self.log_execution_skip(
                reason="no_nonconflicting_signal_capacity",
                open_trade_count=len(trades),
                candidate_blocks=selection_blocks[:12],
                **blocked_fields(selection_blocks),
            )
            return
        second_curve_entry_veto_enabled = bool(
            getattr(self.args, "execution_second_curve_entry_veto", False)
        )
        second_timing = (
            self.second_curve_state(
                str(selected.get("instrument") or ""),
                str(selected.get("direction") or ""),
            )
            if second_curve_entry_veto_enabled
            else {"state": "disabled"}
        )
        if second_timing.get("state") in {"opposed", "cost_blocked"}:
            timing_state = str(second_timing.get("state") or "")
            self.log_execution_skip(
                reason=(
                    "second_curve_opposed_entry"
                    if timing_state == "opposed"
                    else "second_curve_cost_blocked_entry"
                ),
                selected_id=selected.get("id"),
                instrument=selected.get("instrument"),
                direction=selected.get("direction"),
                second_curve=second_timing,
            )
            return
        local_fill_age = time.monotonic() - self.last_entry_monotonic
        shared_fill_age = self.signal_feed.seconds_since_last_fill() if self.signal_feed is not None else math.inf
        if min(local_fill_age, shared_fill_age) < self.args.execution_cooldown_sec:
            self.log_execution_skip(reason="cooldown", selected_id=selected["id"])
            return
        quote_time = parse_rfc3339(str(selected.get("entry_time") or ""))
        quote_age = (datetime.now(timezone.utc) - quote_time).total_seconds() if quote_time else math.inf
        if quote_age > self.args.execution_max_quote_age_sec:
            self.log_execution_skip(reason="stale_quote", quote_age_sec=round(quote_age, 3))
            return
        summary = self.client.get(f"/v3/accounts/{self.account_id}/summary", params={}).get("account") or {}
        balance = safe_float(summary.get("balance"))
        if self.start_balance - balance >= self.args.execution_max_loss_account:
            self.disabled_reason = "session_loss_limit"
            log_line(
                self.log_path,
                "practice_execution_disabled",
                reason=self.disabled_reason,
                start_balance=self.start_balance,
                balance=balance,
            )
            return

        descriptor = self.acquire_execution_lock()
        if descriptor is None:
            self.log_execution_skip(reason="account_execution_lock_timeout")
            return
        try:
            self.submit_selected_locked(selected)
        finally:
            self.release_execution_lock(descriptor)

    def log_execution_skip(self, reason: str, **fields: Any) -> bool:
        """Rate-limit repeated idle-cycle diagnostics without hiding state changes."""
        now = time.monotonic()
        normalized_reason = str(reason or "unknown")
        if (
            normalized_reason == self.last_execution_skip_reason
            and now - self.last_execution_skip_log_monotonic < 10.0
        ):
            return False
        log_line(
            self.log_path,
            "execution_skipped",
            reason=normalized_reason,
            **fields,
        )
        self.last_execution_skip_reason = normalized_reason
        self.last_execution_skip_log_monotonic = now
        return True


class MultiPriceStream:
    """Maintain the latest executable quote for every requested instrument."""

    def __init__(
        self,
        credential_provider: Callable[[], tuple[str, str]],
        instruments: list[str],
        log: Callable[..., None],
        reconnect_base_sec: float = 0.5,
        research_snapshot_path: Path | None = None,
        research_snapshot_interval_sec: float = 5.0,
        research_snapshot_producer: str = "strategy_lab_price_stream",
        research_snapshot_min_instruments: int = 1,
        research_snapshot_seed_paths: list[Path] | None = None,
        pip_sizes: dict[str, float] | None = None,
        quote_observer: Callable[..., None] | None = None,
        raw_price_observer: Callable[..., None] | None = None,
    ) -> None:
        self.credential_provider = credential_provider
        self.instruments = instruments
        self.log = log
        self.reconnect_base_sec = max(0.25, reconnect_base_sec)
        self.research_snapshot_path = research_snapshot_path
        self.research_snapshot_interval_sec = max(
            1.0, research_snapshot_interval_sec
        )
        self.research_snapshot_producer = str(
            research_snapshot_producer or "strategy_lab_price_stream"
        )
        self.research_snapshot_min_instruments = max(
            1,
            int(research_snapshot_min_instruments),
        )
        self.research_snapshot_seed_paths = [
            Path(path) for path in (research_snapshot_seed_paths or [])
        ]
        self.pip_sizes = dict(pip_sizes or {})
        self.quote_observer = quote_observer
        # Opt-in market-data evidence hook; existing float consumers unchanged.
        self.raw_price_observer = raw_price_observer
        self._raw_price_observer_errors = 0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._quotes: dict[str, Any] = {}
        self._metadata: dict[str, dict[str, Any]] = {}
        self._thread: threading.Thread | None = None
        self._response: requests.Response | None = None
        self._connected = False
        self._connection_generation = 0
        self._last_event_monotonic = 0.0
        self._updates = 0
        self._reconnects = 0
        self._quote_observer_errors = 0
        self._broker_clock_leads_sec: deque[float] = deque(maxlen=128)
        self._last_research_snapshot_monotonic = -math.inf
        self._last_research_snapshot_quote_count = 0
        self._research_cache_lock = threading.Lock()
        self._research_retained_quotes: dict[str, dict[str, Any]] = {}
        self._seed_research_snapshot_cache()
        self._research_snapshot_publisher = (
            QuoteSnapshotPublisher(self.research_snapshot_path)
            if self.research_snapshot_path is not None
            else None
        )

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="oanda-all-pairs-price-stream", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        response = self._response
        if response is not None:
            try:
                response.close()
            except Exception:
                pass
        if self._thread:
            self._thread.join(timeout=7.0)
        if self._research_snapshot_publisher is not None:
            self._research_snapshot_publisher.close(timeout_sec=5.0)

    def _activate_connection(self, now: float | None = None) -> tuple[int, bool]:
        """Start a clean quote generation after a successful stream connect.

        OANDA sends a fresh price snapshot on every connection.  Quotes left
        in ``_quotes`` by the prior connection are therefore not current in
        the new generation, even if their individual timestamps have not yet
        exceeded the execution freshness limit.  Preserve them only in the
        explicitly research-only last-known cache and fail closed until the
        new snapshot repopulates the executable map.
        """
        connected_at = time.monotonic() if now is None else float(now)
        with self._lock:
            is_reconnect = self._connection_generation > 0
            prior_quotes = dict(self._quotes) if is_reconnect else {}
            if is_reconnect:
                self._quotes.clear()
                self._metadata.clear()
                self._last_research_snapshot_quote_count = 0
                self._reconnects += 1
            self._connection_generation += 1
            generation = self._connection_generation
            self._connected = True
            self._last_event_monotonic = connected_at

        if prior_quotes:
            retained_rows = {
                instrument: {
                    "bid": safe_float(quote.bid),
                    "ask": safe_float(quote.ask),
                    "time": str(quote.time or ""),
                    "pip": safe_float(
                        self.pip_sizes.get(
                            instrument,
                            0.01 if instrument.endswith("_JPY") else 0.0001,
                        )
                    ),
                    "source": str(
                        getattr(quote, "source", "") or "strategy_price_stream"
                    ),
                    "tradeable": bool(getattr(quote, "tradeable", True)),
                }
                for instrument, quote in prior_quotes.items()
            }
            with self._research_cache_lock:
                for instrument, row in retained_rows.items():
                    existing = self._research_retained_quotes.get(instrument)
                    existing_time = parse_rfc3339(str((existing or {}).get("time") or ""))
                    incoming_time = parse_rfc3339(str(row.get("time") or ""))
                    if existing_time is None or (
                        incoming_time is not None and incoming_time >= existing_time
                    ):
                        self._research_retained_quotes[instrument] = row
        self._notify_raw_price(None, time.time(), generation)
        return generation, is_reconnect

    def _notify_raw_price(self, raw: bytes | None, received_epoch: float, generation: int) -> None:
        if self.raw_price_observer is not None:
            try:
                self.raw_price_observer(raw, received_epoch=received_epoch,
                                        connection_generation=generation)
            except Exception:
                with self._lock:
                    self._raw_price_observer_errors += 1

    def publish_research_snapshot(self, payload: dict[str, Any]) -> int | None:
        """Queue diagnostic coverage without making retained quotes executable.

        Some instruments stop quoting before the broad Friday close.  A restart
        must not erase those last-known research observations, but stale rows
        must never enter ``_quotes`` or the execution freshness path.  The
        coverage metadata makes that distinction explicit for every consumer.
        """
        if self._research_snapshot_publisher is None:
            return None
        snapshot = dict(payload)
        current_quotes = {
            str(name): dict(row)
            for name, row in (payload.get("quotes") or {}).items()
            if str(name) in self.instruments and isinstance(row, dict)
        }
        current_tradeable = sorted(
            name
            for name, row in current_quotes.items()
            if row.get("tradeable") is True
        )
        current_non_tradeable = sorted(
            name
            for name, row in current_quotes.items()
            if row.get("tradeable") is False
        )
        current_tradeability_unknown = sorted(
            name
            for name, row in current_quotes.items()
            if not isinstance(row.get("tradeable"), bool)
        )
        with self._research_cache_lock:
            self._research_retained_quotes.update(current_quotes)
            retained_only = sorted(
                set(self._research_retained_quotes).difference(current_quotes)
            )
            merged_quotes = {
                name: dict(row)
                for name, row in self._research_retained_quotes.items()
            }
        snapshot["schema_version"] = max(
            3, int(safe_float(snapshot.get("schema_version"), 1.0))
        )
        with self._lock:
            connection_generation = self._connection_generation
        snapshot["connection_generation"] = int(
            safe_float(
                snapshot.get("connection_generation"),
                float(connection_generation),
            )
        )
        snapshot["quotes"] = merged_quotes
        snapshot["quote_count"] = len(merged_quotes)
        snapshot["coverage"] = {
            "current_quote_count": len(current_quotes),
            "last_known_quote_count": len(merged_quotes),
            "retained_last_known_count": len(retained_only),
            "retained_last_known_instruments": retained_only,
            "connection_generation": snapshot["connection_generation"],
            "current_tradeable_quote_count": len(current_tradeable),
            "current_non_tradeable_quote_count": len(current_non_tradeable),
            "current_tradeability_unknown_count": len(
                current_tradeability_unknown
            ),
            "current_non_tradeable_instruments": current_non_tradeable,
            "current_tradeability_unknown_instruments": (
                current_tradeability_unknown
            ),
            "tradeability_contract": "oanda_client_price_status_boolean_v1",
            "retained_quotes_execution_eligible": False,
            "execution_requires_independent_freshness_check": True,
        }
        return self._research_snapshot_publisher.submit(snapshot)

    def _publish_connection_boundary(
        self,
        connection_generation: int,
        *,
        is_reconnect: bool,
    ) -> int | None:
        """Reclassify prior-process/connection rows before a fresh snapshot."""
        with self._research_cache_lock:
            has_retained_rows = bool(self._research_retained_quotes)
        if not is_reconnect and not has_retained_rows:
            return None
        return self.publish_research_snapshot(
            {
                "schema_version": 2,
                "generated_utc": utc_now(),
                "producer": self.research_snapshot_producer,
                "connection_generation": connection_generation,
                "quote_count": 0,
                "quotes": {},
                "research_only": True,
            }
        )

    def _seed_research_snapshot_cache(self) -> None:
        """Load last-known research rows without seeding executable prices."""
        paths: list[Path] = []
        if self.research_snapshot_path is not None:
            paths.append(Path(self.research_snapshot_path))
        paths.extend(self.research_snapshot_seed_paths)
        allowed = set(self.instruments)
        for path in paths:
            payload = load_quote_snapshot(path)
            for instrument, raw in (payload.get("quotes") or {}).items():
                if instrument not in allowed or not isinstance(raw, dict):
                    continue
                bid = safe_float(raw.get("bid"))
                ask = safe_float(raw.get("ask"))
                if bid <= 0.0 or ask < bid or parse_rfc3339(str(raw.get("time") or "")) is None:
                    continue
                existing = self._research_retained_quotes.get(instrument)
                existing_time = parse_rfc3339(str((existing or {}).get("time") or ""))
                incoming_time = parse_rfc3339(str(raw.get("time") or ""))
                if existing_time is None or (
                    incoming_time is not None and incoming_time >= existing_time
                ):
                    self._research_retained_quotes[instrument] = dict(raw)

    def _research_snapshot_due(self, now: float) -> bool:
        return self.research_snapshot_path is not None and (
            now - self._last_research_snapshot_monotonic
            >= self.research_snapshot_interval_sec
            or len(self._quotes) > self._last_research_snapshot_quote_count
        )

    def wait_ready(self, timeout_sec: float) -> bool:
        return self._ready.wait(max(0.0, timeout_sec))

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._quotes)

    def snapshot_metadata(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {instrument: dict(row) for instrument, row in self._metadata.items()}

    def healthy(self, max_silence_sec: float = 12.0) -> bool:
        with self._lock:
            silence = time.monotonic() - self._last_event_monotonic if self._last_event_monotonic else math.inf
            return self._connected and silence <= max_silence_sec

    def stats(self) -> dict[str, Any]:
        with self._lock:
            silence = time.monotonic() - self._last_event_monotonic if self._last_event_monotonic else math.inf
            clock_lead = (
                statistics.median(self._broker_clock_leads_sec)
                if self._broker_clock_leads_sec
                else None
            )
            stats = {
                "connected": self._connected,
                "quoted_instruments": len(self._quotes),
                "metadata_instruments": len(self._metadata),
                "updates": self._updates,
                "reconnects": self._reconnects,
                "connection_generation": self._connection_generation,
                "quote_observer_errors": self._quote_observer_errors,
                "raw_price_observer_errors": self._raw_price_observer_errors,
                "last_event_age_sec": None if not math.isfinite(silence) else round(silence, 3),
                "broker_clock_lead_sec": (
                    None if clock_lead is None else round(clock_lead, 3)
                ),
                "broker_clock_sample_count": len(self._broker_clock_leads_sec),
                "clock_sync_status": (
                    "unknown"
                    if clock_lead is None
                    else "local_clock_behind_broker"
                    if clock_lead > 2.0
                    else "local_clock_ahead_of_broker"
                    if clock_lead < -2.0
                    else "aligned"
                ),
            }
        stats["research_snapshot_transport"] = (
            {"enabled": False}
            if self._research_snapshot_publisher is None
            else self._research_snapshot_publisher.stats()
        )
        return stats

    def _run(self) -> None:
        failures = 0
        while not self._stop.is_set():
            response: requests.Response | None = None
            try:
                token, account_id = self.credential_provider()
                response = requests.get(
                    f"{STREAM_URL}/v3/accounts/{account_id}/pricing/stream",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Accept-Datetime-Format": "RFC3339",
                    },
                    params={
                        "instruments": ",".join(self.instruments),
                        "snapshot": "true",
                        "includeHomeConversions": "false",
                    },
                    stream=True,
                    timeout=(10, 15),
                )
                self._response = response
                if response.status_code >= 400:
                    failures += 1
                    self.log(
                        "lab_price_stream_http_error",
                        status=response.status_code,
                        request_id=str(response.headers.get("RequestID") or ""),
                    )
                else:
                    failures = 0
                    connection_generation, is_reconnect = self._activate_connection()
                    # Publish the generation boundary immediately.  This also
                    # covers a process restart seeded from the prior on-disk
                    # snapshot.  Until fresh OANDA rows arrive, every old row
                    # is retained research context, never current coverage.
                    self._publish_connection_boundary(
                        connection_generation,
                        is_reconnect=is_reconnect,
                    )
                    self.log(
                        "lab_price_stream_connected",
                        instrument_count=len(self.instruments),
                        connection_generation=connection_generation,
                        reconnect=is_reconnect,
                    )
                    for raw_line in response.iter_lines():
                        if self._stop.is_set():
                            break
                        if not raw_line:
                            continue
                        received_epoch = time.time()
                        try:
                            payload = json.loads(raw_line.decode("utf-8"))
                        except (UnicodeDecodeError, json.JSONDecodeError):
                            continue
                        now = time.monotonic()
                        if str(payload.get("type") or "").upper() == "HEARTBEAT":
                            with self._lock:
                                self._last_event_monotonic = now
                            continue
                        instrument = str(payload.get("instrument") or "")
                        if payload.get("type") == "PRICE":
                            self._notify_raw_price(raw_line, received_epoch, connection_generation)
                        if not instrument or not payload.get("bids") or not payload.get("asks"):
                            continue
                        quote = quote_from_price_payload(payload, "stream")
                        broker_timestamp = parse_rfc3339(str(quote.time or ""))
                        broker_clock_lead = (
                            (broker_timestamp - datetime.now(timezone.utc)).total_seconds()
                            if broker_timestamp is not None
                            else None
                        )
                        bids = list(payload.get("bids") or [])
                        asks = list(payload.get("asks") or [])
                        bid_top = safe_float(bids[0].get("liquidity"))
                        ask_top = safe_float(asks[0].get("liquidity"))
                        bid_total = sum(safe_float(level.get("liquidity")) for level in bids)
                        ask_total = sum(safe_float(level.get("liquidity")) for level in asks)
                        total_depth = bid_total + ask_total
                        top_depth = bid_top + ask_top
                        microprice = (
                            (quote.ask * bid_top + quote.bid * ask_top) / top_depth
                            if top_depth > 0.0
                            else quote.mid
                        )
                        with self._lock:
                            version = int(safe_float((self._metadata.get(instrument) or {}).get("version"))) + 1
                            self._quotes[instrument] = quote
                            self._metadata[instrument] = {
                                "version": version,
                                "connection_generation": self._connection_generation,
                                "received_monotonic": now,
                                "broker_time": quote.time,
                                "bid_top_liquidity": bid_top,
                                "ask_top_liquidity": ask_top,
                                "bid_total_liquidity": bid_total,
                                "ask_total_liquidity": ask_total,
                                "depth_imbalance": (
                                    (bid_total - ask_total) / total_depth if total_depth > 0.0 else 0.0
                                ),
                                "bid_levels": len(bids),
                                "ask_levels": len(asks),
                                "microprice": microprice,
                            }
                            self._last_event_monotonic = now
                            self._updates += 1
                            if (
                                broker_clock_lead is not None
                                and math.isfinite(broker_clock_lead)
                                and abs(broker_clock_lead) <= 600.0
                            ):
                                self._broker_clock_leads_sec.append(
                                    broker_clock_lead
                                )
                            # During the initial OANDA snapshot, the 95%
                            # threshold can be crossed before the final
                            # instruments arrive. Publish each coverage
                            # increase so the async writer coalesces to the
                            # complete universe even if no later tick arrives.
                            write_research_snapshot = self._research_snapshot_due(now)
                            if write_research_snapshot:
                                candidate_quotes = dict(self._quotes)
                                if (
                                    len(candidate_quotes)
                                    >= self.research_snapshot_min_instruments
                                ):
                                    research_quotes = candidate_quotes
                                    self._last_research_snapshot_monotonic = now
                                    self._last_research_snapshot_quote_count = len(
                                        candidate_quotes
                                    )
                                else:
                                    research_quotes = {}
                            else:
                                research_quotes = {}
                        if self.quote_observer is not None:
                            try:
                                pip = safe_float(
                                    self.pip_sizes.get(
                                        instrument,
                                        0.01 if instrument.endswith("_JPY") else 0.0001,
                                    )
                                )
                                self.quote_observer(
                                    instrument,
                                    mid=quote.mid,
                                    spread_pips=(quote.ask - quote.bid) / max(pip, 1e-12),
                                    broker_time=str(quote.time or ""),
                                    received_monotonic=now,
                                )
                            except Exception:
                                with self._lock:
                                    self._quote_observer_errors += 1
                        if research_quotes:
                            self.publish_research_snapshot(
                                {
                                        "schema_version": 1,
                                        "generated_utc": utc_now(),
                                        "producer": self.research_snapshot_producer,
                                        "quote_count": len(research_quotes),
                                        "quotes": {
                                            name: {
                                                "bid": safe_float(value.bid),
                                                "ask": safe_float(value.ask),
                                                "time": str(value.time or ""),
                                                "pip": safe_float(
                                                    self.pip_sizes.get(
                                                        name,
                                                        0.01
                                                        if name.endswith("_JPY")
                                                        else 0.0001,
                                                    )
                                                ),
                                                "source": str(
                                                    getattr(value, "source", "")
                                                    or "strategy_price_stream"
                                                ),
                                                "tradeable": bool(
                                                    getattr(
                                                        value,
                                                        "tradeable",
                                                        True,
                                                    )
                                                ),
                                            }
                                            for name, value in research_quotes.items()
                                        },
                                        "research_only": True,
                                }
                            )
                        self._ready.set()
            except requests.RequestException as exc:
                failures += 1
                if not self._stop.is_set():
                    self.log("lab_price_stream_error", error_type=type(exc).__name__, error=str(exc)[:300])
            except Exception as exc:
                failures += 1
                if not self._stop.is_set():
                    self.log("lab_price_stream_error", error_type=type(exc).__name__, error=str(exc)[:300])
            finally:
                with self._lock:
                    self._connected = False
                if response is not None:
                    response.close()
                self._response = None
            if self._stop.is_set():
                break
            delay = min(15.0, self.reconnect_base_sec * (2 ** min(failures, 5)))
            self._stop.wait(delay)


GATE_PROFILES: dict[str, dict[str, float]] = {
    "strict": {
        "max_spread_pips": 1.0,
        "max_spread_atr_fraction": 0.30,
        "min_reward_to_spread": 6.0,
        "min_signal_to_spread": 3.0,
        "stop_loss_pips": 5.0,
        "take_profit_r": 1.6,
    },
    "balanced": {
        "max_spread_pips": 1.8,
        "max_spread_atr_fraction": 0.55,
        "min_reward_to_spread": 4.0,
        "min_signal_to_spread": 2.0,
        "stop_loss_pips": 5.0,
        "take_profit_r": 1.6,
    },
    "fast": {
        "max_spread_pips": 2.2,
        "max_spread_atr_fraction": 0.70,
        "min_reward_to_spread": 3.0,
        "min_signal_to_spread": 1.4,
        "stop_loss_pips": 4.0,
        "take_profit_r": 1.5,
    },
    "loose": {
        "max_spread_pips": 3.0,
        "max_spread_atr_fraction": 1.00,
        "min_reward_to_spread": 2.0,
        "min_signal_to_spread": 0.8,
        "stop_loss_pips": 5.0,
        "take_profit_r": 1.4,
    },
}


FAMILY_PARAMETERS: dict[str, dict[str, dict[str, float | int | str]]] = {
    "momentum": {
        "strict": {"min_r1": 0.30, "min_r3": 3.0, "min_r5": 2.0, "min_m5_r3": 0.60, "pos_long": 0.75, "pos_short": 0.25},
        "balanced": {"min_r1": 0.10, "min_r3": 2.0, "min_r5": 1.0, "min_m5_r3": 0.30, "pos_long": 0.65, "pos_short": 0.35},
        "fast": {"min_r1": 0.05, "min_r3": 1.2, "min_r5": 0.7, "min_m5_r3": 0.15, "pos_long": 0.58, "pos_short": 0.42},
        "loose": {"min_r1": 0.00, "min_r3": 0.8, "min_r5": 0.3, "min_m5_r3": 0.00, "pos_long": 0.52, "pos_short": 0.48},
    },
    "ahl_multihorizon_trend": {
        "balanced": {
            "lookbacks_days": "5,10,21,42",
            "volatility_lookback_days": 20,
            "target_annualized_volatility": 0.10,
            "min_abs_score": 2,
        },
    },
    "pullback": {
        "strict": {"m1_ema": 12, "m5_fast": 12, "m5_slow": 26, "min_m5_r3": 0.60, "min_r1": 0.20, "pos_low": 0.40, "pos_high": 0.85},
        "balanced": {"m1_ema": 9, "m5_fast": 8, "m5_slow": 21, "min_m5_r3": 0.30, "min_r1": 0.10, "pos_low": 0.35, "pos_high": 0.90},
        "fast": {"m1_ema": 5, "m5_fast": 5, "m5_slow": 13, "min_m5_r3": 0.15, "min_r1": 0.05, "pos_low": 0.25, "pos_high": 0.95},
        "loose": {"m1_ema": 7, "m5_fast": 5, "m5_slow": 13, "min_m5_r3": 0.00, "min_r1": 0.00, "pos_low": 0.10, "pos_high": 0.98},
    },
    "macd_rsi_reversal": {
        "strict": {"rsi_low": 30.0, "rsi_high": 70.0, "context_low": 42.0, "context_high": 58.0},
        "balanced": {"rsi_low": 35.0, "rsi_high": 65.0, "context_low": 45.0, "context_high": 55.0},
        "fast": {"rsi_low": 40.0, "rsi_high": 60.0, "context_low": 48.0, "context_high": 52.0},
        "loose": {"rsi_low": 45.0, "rsi_high": 55.0, "context_low": 52.0, "context_high": 48.0},
    },
    "ema_trend_cross": {
        "strict": {"m1_fast": 12, "m1_slow": 26, "m5_fast": 12, "m5_slow": 30, "min_m5_r3": 0.50},
        "balanced": {"m1_fast": 8, "m1_slow": 21, "m5_fast": 8, "m5_slow": 21, "min_m5_r3": 0.25},
        "fast": {"m1_fast": 5, "m1_slow": 13, "m5_fast": 5, "m5_slow": 13, "min_m5_r3": 0.10},
        "loose": {"m1_fast": 3, "m1_slow": 9, "m5_fast": 5, "m5_slow": 13, "min_m5_r3": 0.00},
    },
    "bollinger_reversion": {
        "strict": {"period": 24, "z": 2.2, "rsi_low": 32.0, "rsi_high": 68.0},
        "balanced": {"period": 20, "z": 2.0, "rsi_low": 38.0, "rsi_high": 62.0},
        "fast": {"period": 14, "z": 1.7, "rsi_low": 44.0, "rsi_high": 56.0},
        "loose": {"period": 12, "z": 1.4, "rsi_low": 50.0, "rsi_high": 50.0},
    },
    "donchian_breakout": {
        "strict": {"period": 55, "buffer_pips": 0.30, "min_m5_r3": 0.50},
        "balanced": {"period": 30, "buffer_pips": 0.10, "min_m5_r3": 0.25},
        "fast": {"period": 20, "buffer_pips": 0.00, "min_m5_r3": 0.10},
        "loose": {"period": 12, "buffer_pips": 0.00, "min_m5_r3": 0.00},
    },
    "currency_strength": {
        "strict": {"min_cross_samples": 4, "min_strength_r3": 0.55, "min_strength_r5": 0.70, "min_breadth": 0.35, "min_pair_r1": 0.10, "min_pair_r3": 0.40},
        "balanced": {"min_cross_samples": 3, "min_strength_r3": 0.40, "min_strength_r5": 0.50, "min_breadth": 0.25, "min_pair_r1": 0.05, "min_pair_r3": 0.25},
        "fast": {"min_cross_samples": 3, "min_strength_r3": 0.28, "min_strength_r5": 0.32, "min_breadth": 0.12, "min_pair_r1": 0.02, "min_pair_r3": 0.15},
        "loose": {"min_cross_samples": 2, "min_strength_r3": 0.18, "min_strength_r5": 0.18, "min_breadth": 0.00, "min_pair_r1": 0.00, "min_pair_r3": 0.05},
    },
    "relative_value_reversion": {
        "strict": {"min_cross_samples": 4, "min_residual_r3": 1.10, "min_residual_r5": 0.70, "min_reversal_r1": 0.15, "max_system_strength": 0.45, "long_pos_max": 0.40, "short_pos_min": 0.60},
        "balanced": {"min_cross_samples": 3, "min_residual_r3": 0.90, "min_residual_r5": 0.55, "min_reversal_r1": 0.10, "max_system_strength": 0.60, "long_pos_max": 0.50, "short_pos_min": 0.50},
        "fast": {"min_cross_samples": 3, "min_residual_r3": 0.70, "min_residual_r5": 0.40, "min_reversal_r1": 0.05, "max_system_strength": 0.80, "long_pos_max": 0.60, "short_pos_min": 0.40},
        "loose": {"min_cross_samples": 2, "min_residual_r3": 0.55, "min_residual_r5": 0.25, "min_reversal_r1": 0.00, "max_system_strength": 1.00, "long_pos_max": 0.70, "short_pos_min": 0.30},
    },
    "supervised_return_rank": {
        "strict": {"min_model_rows": 5000, "min_rank_percentile": 0.97, "min_expected_net_pips": 1.50, "min_validation_hit_rate": 0.45, "min_validation_avg_norm": 0.00},
        "balanced": {"min_model_rows": 3000, "min_rank_percentile": 0.93, "min_expected_net_pips": 1.00, "min_validation_hit_rate": 0.35, "min_validation_avg_norm": -0.05},
        "fast": {"min_model_rows": 1500, "min_rank_percentile": 0.85, "min_expected_net_pips": 0.50, "min_validation_hit_rate": 0.25, "min_validation_avg_norm": -0.15},
        "loose": {"min_model_rows": 500, "min_rank_percentile": 0.985, "min_expected_net_pips": -99.00, "min_validation_hit_rate": 0.00, "min_validation_avg_norm": -99.00},
    },
    "higher_timeframe_alignment": {
        "strict": {"m1_fast": 12, "m1_slow": 26, "m15_fast": 12, "m15_slow": 26, "h1_fast": 8, "h1_slow": 21, "max_cross_age": 1, "min_r1": 0.20, "min_m15_gap_atr": 0.12, "min_h1_gap_atr": 0.18},
        "balanced": {"m1_fast": 8, "m1_slow": 21, "m15_fast": 8, "m15_slow": 21, "h1_fast": 6, "h1_slow": 18, "max_cross_age": 2, "min_r1": 0.10, "min_m15_gap_atr": 0.08, "min_h1_gap_atr": 0.12},
        "fast": {"m1_fast": 5, "m1_slow": 13, "m15_fast": 5, "m15_slow": 13, "h1_fast": 5, "h1_slow": 13, "max_cross_age": 3, "min_r1": 0.05, "min_m15_gap_atr": 0.04, "min_h1_gap_atr": 0.06},
        "loose": {"m1_fast": 3, "m1_slow": 9, "m15_fast": 5, "m15_slow": 13, "h1_fast": 4, "h1_slow": 10, "max_cross_age": 5, "min_r1": 0.00, "min_m15_gap_atr": 0.00, "min_h1_gap_atr": 0.00},
    },
    "rsi_trend_continuation": {
        "strict": {"ema_fast": 12, "ema_slow": 26, "rsi_trigger": 55.0, "max_rsi": 68.0, "min_r1": 0.20, "min_m5_r3": 0.50},
        "balanced": {"ema_fast": 8, "ema_slow": 21, "rsi_trigger": 52.0, "max_rsi": 72.0, "min_r1": 0.10, "min_m5_r3": 0.25},
        "fast": {"ema_fast": 5, "ema_slow": 13, "rsi_trigger": 50.0, "max_rsi": 76.0, "min_r1": 0.05, "min_m5_r3": 0.10},
        "loose": {"ema_fast": 3, "ema_slow": 9, "rsi_trigger": 48.0, "max_rsi": 82.0, "min_r1": 0.00, "min_m5_r3": 0.00},
    },
    "stochastic_reversal": {
        "strict": {"period": 14, "smooth": 3, "oversold": 20.0, "overbought": 80.0, "min_r1": 0.20, "long_pos_max": 0.30, "short_pos_min": 0.70},
        "balanced": {"period": 14, "smooth": 3, "oversold": 25.0, "overbought": 75.0, "min_r1": 0.10, "long_pos_max": 0.40, "short_pos_min": 0.60},
        "fast": {"period": 10, "smooth": 3, "oversold": 30.0, "overbought": 70.0, "min_r1": 0.05, "long_pos_max": 0.50, "short_pos_min": 0.50},
        "loose": {"period": 8, "smooth": 2, "oversold": 35.0, "overbought": 65.0, "min_r1": 0.00, "long_pos_max": 0.60, "short_pos_min": 0.40},
    },
    "volatility_squeeze_breakout": {
        "strict": {"squeeze_period": 12, "baseline_period": 40, "breakout_period": 12, "max_width_ratio": 0.45, "min_r1": 0.30, "min_m5_r3": 0.50},
        "balanced": {"squeeze_period": 10, "baseline_period": 36, "breakout_period": 10, "max_width_ratio": 0.60, "min_r1": 0.15, "min_m5_r3": 0.25},
        "fast": {"squeeze_period": 8, "baseline_period": 30, "breakout_period": 7, "max_width_ratio": 0.80, "min_r1": 0.05, "min_m5_r3": 0.10},
        "loose": {"squeeze_period": 6, "baseline_period": 24, "breakout_period": 4, "max_width_ratio": 1.10, "min_r1": 0.00, "min_m5_r3": 0.00},
    },
    "range_expansion": {
        "strict": {"lookback": 24, "min_range_atr": 1.80, "min_body_fraction": 0.70, "close_extreme": 0.85, "min_r1": 0.20},
        "balanced": {"lookback": 20, "min_range_atr": 1.40, "min_body_fraction": 0.60, "close_extreme": 0.80, "min_r1": 0.10},
        "fast": {"lookback": 16, "min_range_atr": 1.10, "min_body_fraction": 0.50, "close_extreme": 0.75, "min_r1": 0.05},
        "loose": {"lookback": 12, "min_range_atr": 0.85, "min_body_fraction": 0.40, "close_extreme": 0.65, "min_r1": 0.00},
    },
    "candlestick_reversal": {
        "strict": {"min_body_pips": 0.30, "min_wick_body": 3.00, "long_pos_max": 0.30, "short_pos_min": 0.70, "min_r1": 0.20},
        "balanced": {"min_body_pips": 0.20, "min_wick_body": 2.00, "long_pos_max": 0.40, "short_pos_min": 0.60, "min_r1": 0.10},
        "fast": {"min_body_pips": 0.10, "min_wick_body": 1.50, "long_pos_max": 0.50, "short_pos_min": 0.50, "min_r1": 0.05},
        "loose": {"min_body_pips": 0.05, "min_wick_body": 1.00, "long_pos_max": 0.60, "short_pos_min": 0.40, "min_r1": 0.00},
    },
    "linear_regression_trend": {
        "strict": {"period": 40, "min_slope_pips": 0.12, "min_r2": 0.75, "min_r1": 0.20},
        "balanced": {"period": 30, "min_slope_pips": 0.08, "min_r2": 0.60, "min_r1": 0.10},
        "fast": {"period": 20, "min_slope_pips": 0.05, "min_r2": 0.45, "min_r1": 0.05},
        "loose": {"period": 12, "min_slope_pips": 0.02, "min_r2": 0.25, "min_r1": 0.00},
    },
    "volume_impulse": {
        "strict": {"lookback": 30, "min_volume_ratio": 2.00, "min_body_atr": 0.60, "close_extreme": 0.85},
        "balanced": {"lookback": 24, "min_volume_ratio": 1.60, "min_body_atr": 0.45, "close_extreme": 0.80},
        "fast": {"lookback": 18, "min_volume_ratio": 1.30, "min_body_atr": 0.30, "close_extreme": 0.72},
        "loose": {"lookback": 12, "min_volume_ratio": 1.05, "min_body_atr": 0.15, "close_extreme": 0.62},
    },
    "atr_mean_reversion": {
        "strict": {"period": 24, "entry_atr": 1.80, "min_reversal_pips": 0.30, "max_m5_trend_atr": 0.40},
        "balanced": {"period": 20, "entry_atr": 1.40, "min_reversal_pips": 0.15, "max_m5_trend_atr": 0.60},
        "fast": {"period": 14, "entry_atr": 1.10, "min_reversal_pips": 0.05, "max_m5_trend_atr": 0.85},
        "loose": {"period": 10, "entry_atr": 0.80, "min_reversal_pips": 0.00, "max_m5_trend_atr": 1.20},
    },
    "cross_sectional_pair_rank": {
        "strict": {"min_pairs": 30, "min_rank": 0.97, "min_r1": 0.20},
        "balanced": {"min_pairs": 24, "min_rank": 0.92, "min_r1": 0.10},
        "fast": {"min_pairs": 16, "min_rank": 0.84, "min_r1": 0.05},
        "loose": {"min_pairs": 8, "min_rank": 0.72, "min_r1": 0.00},
    },
    "regime_switching": {
        "strict": {"period": 30, "trend_er": 0.68, "trend_min_r3": 2.00, "reversion_er": 0.22, "reversion_z": 1.80, "min_r1": 0.20},
        "balanced": {"period": 24, "trend_er": 0.55, "trend_min_r3": 1.30, "reversion_er": 0.32, "reversion_z": 1.50, "min_r1": 0.10},
        "fast": {"period": 18, "trend_er": 0.42, "trend_min_r3": 0.80, "reversion_er": 0.42, "reversion_z": 1.20, "min_r1": 0.05},
        "loose": {"period": 12, "trend_er": 0.30, "trend_min_r3": 0.40, "reversion_er": 0.55, "reversion_z": 0.90, "min_r1": 0.00},
    },
    "pattern_count_forecast": {
        "strict": {"pattern_mode": "sign", "pattern_order": 7, "target_horizon_min": 5, "min_pattern_count": 80, "min_direction_edge": 0.08, "min_movement_coefficient": 1.15},
        "balanced": {"pattern_mode": "sign", "pattern_order": 5, "target_horizon_min": 3, "min_pattern_count": 50, "min_direction_edge": 0.05, "min_movement_coefficient": 1.05},
        "fast": {"pattern_mode": "sign", "pattern_order": 3, "target_horizon_min": 1, "min_pattern_count": 30, "min_direction_edge": 0.03, "min_movement_coefficient": 0.95},
        "loose": {"pattern_mode": "magnitude", "pattern_order": 3, "target_horizon_min": 1, "min_pattern_count": 20, "min_direction_edge": 0.00, "min_movement_coefficient": 0.00},
    },
    "failed_breakout_reversal": {
        "strict": {"lookback": 24, "breakout_buffer_pips": 0.20, "min_rejection_body_fraction": 0.55, "min_tail_atr": 0.45, "long_pos_max": 0.35, "short_pos_min": 0.65},
        "balanced": {"lookback": 20, "breakout_buffer_pips": 0.10, "min_rejection_body_fraction": 0.45, "min_tail_atr": 0.35, "long_pos_max": 0.45, "short_pos_min": 0.55},
        "fast": {"lookback": 16, "breakout_buffer_pips": 0.00, "min_rejection_body_fraction": 0.35, "min_tail_atr": 0.25, "long_pos_max": 0.55, "short_pos_min": 0.45},
        "loose": {"lookback": 12, "breakout_buffer_pips": 0.00, "min_rejection_body_fraction": 0.25, "min_tail_atr": 0.15, "long_pos_max": 0.65, "short_pos_min": 0.35},
    },
    "efficiency_filtered_momentum": {
        "strict": {"period": 24, "min_efficiency": 0.62, "min_r3": 2.2, "min_r5": 2.8, "max_displacement_atr": 1.6, "min_m5_r3": 0.45},
        "balanced": {"period": 20, "min_efficiency": 0.52, "min_r3": 1.5, "min_r5": 2.0, "max_displacement_atr": 2.0, "min_m5_r3": 0.25},
        "fast": {"period": 16, "min_efficiency": 0.42, "min_r3": 1.0, "min_r5": 1.4, "max_displacement_atr": 2.4, "min_m5_r3": 0.10},
        "loose": {"period": 12, "min_efficiency": 0.34, "min_r3": 0.7, "min_r5": 1.0, "max_displacement_atr": 3.0, "min_m5_r3": 0.00},
    },
    "cross_pair_lead_lag": {
        "strict": {"min_cross_samples": 4, "min_lead_r1": 0.42, "min_lead_r3": 0.50, "min_breadth": 0.28, "max_pair_r1_abs": 0.35, "min_volume_ratio": 1.35},
        "balanced": {"min_cross_samples": 3, "min_lead_r1": 0.30, "min_lead_r3": 0.35, "min_breadth": 0.18, "max_pair_r1_abs": 0.55, "min_volume_ratio": 1.15},
        "fast": {"min_cross_samples": 3, "min_lead_r1": 0.20, "min_lead_r3": 0.24, "min_breadth": 0.08, "max_pair_r1_abs": 0.80, "min_volume_ratio": 0.95},
        "loose": {"min_cross_samples": 2, "min_lead_r1": 0.12, "min_lead_r3": 0.14, "min_breadth": 0.00, "max_pair_r1_abs": 1.20, "min_volume_ratio": 0.75},
    },
    "spread_compression_momentum": {
        "strict": {"min_r1": 0.20, "min_r3": 1.50, "min_volume_ratio": 1.40, "max_spread_ratio": 0.70, "min_spread_drop": 0.20, "min_close_extreme": 0.78},
        "balanced": {"min_r1": 0.10, "min_r3": 1.00, "min_volume_ratio": 1.20, "max_spread_ratio": 0.85, "min_spread_drop": 0.10, "min_close_extreme": 0.70},
        "fast": {"min_r1": 0.05, "min_r3": 0.65, "min_volume_ratio": 1.00, "max_spread_ratio": 1.00, "min_spread_drop": 0.00, "min_close_extreme": 0.62},
        "loose": {"min_r1": 0.00, "min_r3": 0.35, "min_volume_ratio": 0.85, "max_spread_ratio": 1.20, "min_spread_drop": -0.05, "min_close_extreme": 0.55},
    },
    "kama_adaptive_trend": {
        "strict": {"period": 30, "min_efficiency": 0.62, "min_gap_atr": 0.18, "min_r1": 0.20},
        "balanced": {"period": 24, "min_efficiency": 0.52, "min_gap_atr": 0.12, "min_r1": 0.10},
        "fast": {"period": 18, "min_efficiency": 0.42, "min_gap_atr": 0.08, "min_r1": 0.05},
        "loose": {"period": 12, "min_efficiency": 0.32, "min_gap_atr": 0.04, "min_r1": 0.00},
    },
    "cci_reversion": {
        "strict": {"period": 30, "entry": 170.0, "exit_cross": 90.0, "min_reversal_pips": 0.25},
        "balanced": {"period": 24, "entry": 140.0, "exit_cross": 70.0, "min_reversal_pips": 0.12},
        "fast": {"period": 18, "entry": 110.0, "exit_cross": 50.0, "min_reversal_pips": 0.05},
        "loose": {"period": 14, "entry": 85.0, "exit_cross": 35.0, "min_reversal_pips": 0.00},
    },
    "breakout_retest": {
        "strict": {"lookback": 36, "touch_tolerance_pips": 0.25, "min_reclaim_pips": 0.25, "min_m5_r3": 0.40},
        "balanced": {"lookback": 28, "touch_tolerance_pips": 0.35, "min_reclaim_pips": 0.15, "min_m5_r3": 0.20},
        "fast": {"lookback": 20, "touch_tolerance_pips": 0.50, "min_reclaim_pips": 0.05, "min_m5_r3": 0.05},
        "loose": {"lookback": 14, "touch_tolerance_pips": 0.75, "min_reclaim_pips": 0.00, "min_m5_r3": 0.00},
    },
    "volume_climax_reversal": {
        "strict": {"lookback": 36, "min_volume_ratio": 2.40, "min_tail_atr": 0.55, "long_pos_max": 0.30, "short_pos_min": 0.70},
        "balanced": {"lookback": 28, "min_volume_ratio": 1.90, "min_tail_atr": 0.40, "long_pos_max": 0.38, "short_pos_min": 0.62},
        "fast": {"lookback": 20, "min_volume_ratio": 1.50, "min_tail_atr": 0.28, "long_pos_max": 0.46, "short_pos_min": 0.54},
        "loose": {"lookback": 14, "min_volume_ratio": 1.20, "min_tail_atr": 0.18, "long_pos_max": 0.55, "short_pos_min": 0.45},
    },
    "spread_mean_reversion": {
        "strict": {"min_spread_ratio": 1.80, "min_spread_drop": 0.25, "max_abs_r3": 0.80, "long_pos_max": 0.35, "short_pos_min": 0.65},
        "balanced": {"min_spread_ratio": 1.50, "min_spread_drop": 0.15, "max_abs_r3": 1.20, "long_pos_max": 0.42, "short_pos_min": 0.58},
        "fast": {"min_spread_ratio": 1.25, "min_spread_drop": 0.05, "max_abs_r3": 1.80, "long_pos_max": 0.50, "short_pos_min": 0.50},
        "loose": {"min_spread_ratio": 1.10, "min_spread_drop": 0.00, "max_abs_r3": 2.50, "long_pos_max": 0.58, "short_pos_min": 0.42},
    },
    "volatility_shock_fade": {
        "strict": {"lookback": 36, "min_range_atr": 2.10, "min_reversal_pips": 0.30, "max_m5_trend_atr": 0.35},
        "balanced": {"lookback": 28, "min_range_atr": 1.70, "min_reversal_pips": 0.15, "max_m5_trend_atr": 0.55},
        "fast": {"lookback": 20, "min_range_atr": 1.30, "min_reversal_pips": 0.05, "max_m5_trend_atr": 0.85},
        "loose": {"lookback": 14, "min_range_atr": 1.00, "min_reversal_pips": 0.00, "max_m5_trend_atr": 1.20},
    },
    "session_range_breakout": {
        "strict": {"lookback": 60, "buffer_pips": 0.25, "min_volume_ratio": 1.40, "min_r1": 0.20},
        "balanced": {"lookback": 48, "buffer_pips": 0.15, "min_volume_ratio": 1.20, "min_r1": 0.10},
        "fast": {"lookback": 36, "buffer_pips": 0.05, "min_volume_ratio": 1.00, "min_r1": 0.05},
        "loose": {"lookback": 24, "buffer_pips": 0.00, "min_volume_ratio": 0.85, "min_r1": 0.00},
    },
    "micro_channel_break": {
        "strict": {"lookback": 10, "min_slope_pips": 0.08, "break_buffer_pips": 0.20, "min_r1": 0.18},
        "balanced": {"lookback": 8, "min_slope_pips": 0.05, "break_buffer_pips": 0.12, "min_r1": 0.09},
        "fast": {"lookback": 6, "min_slope_pips": 0.03, "break_buffer_pips": 0.05, "min_r1": 0.04},
        "loose": {"lookback": 5, "min_slope_pips": 0.01, "break_buffer_pips": 0.00, "min_r1": 0.00},
    },
    "trend_momentum_confluence": {
        "strict": {"min_votes": 7, "min_margin": 5, "min_r3": 0.80, "max_rsi": 70.0, "min_volume_ratio": 0.90},
        "balanced": {"min_votes": 6, "min_margin": 4, "min_r3": 0.40, "max_rsi": 74.0, "min_volume_ratio": 0.80},
        "fast": {"min_votes": 5, "min_margin": 3, "min_r3": 0.20, "max_rsi": 78.0, "min_volume_ratio": 0.70},
        "loose": {"min_votes": 4, "min_margin": 2, "min_r3": 0.05, "max_rsi": 82.0, "min_volume_ratio": 0.50},
    },
    "breakout_volume_confluence": {
        "strict": {"lookback": 40, "buffer_pips": 0.20, "min_votes": 6, "min_volume_ratio": 1.40, "min_range_atr": 0.80, "min_m5_r3": 0.40, "max_spread_ratio": 0.90},
        "balanced": {"lookback": 30, "buffer_pips": 0.10, "min_votes": 5, "min_volume_ratio": 1.20, "min_range_atr": 0.60, "min_m5_r3": 0.20, "max_spread_ratio": 1.00},
        "fast": {"lookback": 20, "buffer_pips": 0.00, "min_votes": 4, "min_volume_ratio": 1.00, "min_range_atr": 0.40, "min_m5_r3": 0.10, "max_spread_ratio": 1.15},
        "loose": {"lookback": 12, "buffer_pips": 0.00, "min_votes": 3, "min_volume_ratio": 0.80, "min_range_atr": 0.20, "min_m5_r3": 0.00, "max_spread_ratio": 1.35},
    },
    "oscillator_reversion_confluence": {
        "strict": {"z": 2.00, "rsi_low": 30.0, "rsi_high": 70.0, "stochastic_low": 20.0, "stochastic_high": 80.0, "cci": 150.0, "min_votes": 5, "max_trend_atr": 0.50, "min_reversal_pips": 0.15},
        "balanced": {"z": 1.70, "rsi_low": 35.0, "rsi_high": 65.0, "stochastic_low": 25.0, "stochastic_high": 75.0, "cci": 120.0, "min_votes": 4, "max_trend_atr": 0.75, "min_reversal_pips": 0.08},
        "fast": {"z": 1.40, "rsi_low": 40.0, "rsi_high": 60.0, "stochastic_low": 30.0, "stochastic_high": 70.0, "cci": 90.0, "min_votes": 3, "max_trend_atr": 1.00, "min_reversal_pips": 0.03},
        "loose": {"z": 1.10, "rsi_low": 45.0, "rsi_high": 55.0, "stochastic_low": 35.0, "stochastic_high": 65.0, "cci": 70.0, "min_votes": 2, "max_trend_atr": 1.40, "min_reversal_pips": 0.00},
    },
    "cross_market_confluence": {
        "strict": {"min_cross_samples": 5, "min_votes": 6, "min_margin": 4, "min_strength": 0.45, "min_breadth": 0.25, "min_pair_r3": 0.50},
        "balanced": {"min_cross_samples": 4, "min_votes": 5, "min_margin": 3, "min_strength": 0.32, "min_breadth": 0.15, "min_pair_r3": 0.30},
        "fast": {"min_cross_samples": 3, "min_votes": 4, "min_margin": 2, "min_strength": 0.22, "min_breadth": 0.08, "min_pair_r3": 0.15},
        "loose": {"min_cross_samples": 2, "min_votes": 3, "min_margin": 1, "min_strength": 0.12, "min_breadth": 0.00, "min_pair_r3": 0.05},
    },
    "kalman_local_trend": {
        "strict": {"period": 60, "process_ratio": 0.03, "measurement_ratio": 1.50, "min_trend_pips": 0.10, "min_forecast_pips": 0.18, "max_innovation_z": 2.50},
        "balanced": {"period": 52, "process_ratio": 0.06, "measurement_ratio": 1.20, "min_trend_pips": 0.07, "min_forecast_pips": 0.12, "max_innovation_z": 3.00},
        "fast": {"period": 42, "process_ratio": 0.12, "measurement_ratio": 0.90, "min_trend_pips": 0.04, "min_forecast_pips": 0.07, "max_innovation_z": 3.50},
        "loose": {"period": 30, "process_ratio": 0.22, "measurement_ratio": 0.70, "min_trend_pips": 0.02, "min_forecast_pips": 0.03, "max_innovation_z": 4.50},
    },
    "markov_sign_transition": {
        "strict": {"lookback": 60, "order": 3, "min_transitions": 18, "min_probability_edge": 0.12, "laplace": 1.0},
        "balanced": {"lookback": 52, "order": 3, "min_transitions": 12, "min_probability_edge": 0.09, "laplace": 1.0},
        "fast": {"lookback": 42, "order": 2, "min_transitions": 8, "min_probability_edge": 0.06, "laplace": 1.0},
        "loose": {"lookback": 30, "order": 1, "min_transitions": 5, "min_probability_edge": 0.035, "laplace": 1.0},
    },
    "online_ar_forecast": {
        "strict": {"period": 60, "lags": 5, "ridge": 12.0, "min_rows": 40, "min_forecast_pips": 0.18, "min_r2": 0.08},
        "balanced": {"period": 52, "lags": 4, "ridge": 8.0, "min_rows": 32, "min_forecast_pips": 0.12, "min_r2": 0.04},
        "fast": {"period": 42, "lags": 3, "ridge": 5.0, "min_rows": 24, "min_forecast_pips": 0.07, "min_r2": 0.00},
        "loose": {"period": 30, "lags": 2, "ridge": 3.0, "min_rows": 18, "min_forecast_pips": 0.03, "min_r2": -0.10},
    },
    "variance_ratio_regime": {
        "strict": {"period": 60, "aggregation": 5, "trend_ratio": 1.20, "reversion_ratio": 0.78, "min_move_pips": 0.30, "min_reversal_pips": 0.10},
        "balanced": {"period": 52, "aggregation": 4, "trend_ratio": 1.14, "reversion_ratio": 0.84, "min_move_pips": 0.22, "min_reversal_pips": 0.07},
        "fast": {"period": 42, "aggregation": 3, "trend_ratio": 1.09, "reversion_ratio": 0.89, "min_move_pips": 0.14, "min_reversal_pips": 0.04},
        "loose": {"period": 30, "aggregation": 2, "trend_ratio": 1.04, "reversion_ratio": 0.94, "min_move_pips": 0.08, "min_reversal_pips": 0.02},
    },
    "permutation_entropy_momentum": {
        "strict": {"period": 60, "embedding": 4, "max_entropy": 0.72, "min_move_pips": 0.35},
        "balanced": {"period": 52, "embedding": 4, "max_entropy": 0.80, "min_move_pips": 0.25},
        "fast": {"period": 42, "embedding": 3, "max_entropy": 0.87, "min_move_pips": 0.15},
        "loose": {"period": 30, "embedding": 3, "max_entropy": 0.94, "min_move_pips": 0.08},
    },
    "cusum_breakout": {
        "strict": {"period": 60, "drift_z": 0.50, "threshold_z": 5.0, "min_last_z": 0.35},
        "balanced": {"period": 52, "drift_z": 0.40, "threshold_z": 4.0, "min_last_z": 0.25},
        "fast": {"period": 42, "drift_z": 0.30, "threshold_z": 3.0, "min_last_z": 0.15},
        "loose": {"period": 30, "drift_z": 0.20, "threshold_z": 2.0, "min_last_z": 0.08},
    },
    "har_volatility_momentum": {
        "strict": {"period": 60, "short_window": 5, "medium_window": 15, "long_window": 45, "ridge": 4.0, "min_forecast_ratio": 1.35, "min_move_pips": 0.35},
        "balanced": {"period": 56, "short_window": 5, "medium_window": 12, "long_window": 36, "ridge": 3.0, "min_forecast_ratio": 1.22, "min_move_pips": 0.25},
        "fast": {"period": 48, "short_window": 4, "medium_window": 10, "long_window": 30, "ridge": 2.0, "min_forecast_ratio": 1.12, "min_move_pips": 0.15},
        "loose": {"period": 36, "short_window": 3, "medium_window": 8, "long_window": 24, "ridge": 1.0, "min_forecast_ratio": 1.04, "min_move_pips": 0.08},
    },
    "bipower_jump_reversal": {
        "strict": {"period": 60, "min_jump_z": 3.50, "min_jump_share": 0.35, "min_reversal_pips": 0.20},
        "balanced": {"period": 52, "min_jump_z": 3.00, "min_jump_share": 0.28, "min_reversal_pips": 0.12},
        "fast": {"period": 42, "min_jump_z": 2.50, "min_jump_share": 0.20, "min_reversal_pips": 0.07},
        "loose": {"period": 30, "min_jump_z": 2.00, "min_jump_share": 0.12, "min_reversal_pips": 0.03},
    },
    "garch_volatility_breakout": {
        "strict": {"period": 60, "alpha": 0.08, "beta": 0.88, "min_volatility_ratio": 1.35, "min_move_pips": 0.35},
        "balanced": {"period": 52, "alpha": 0.10, "beta": 0.84, "min_volatility_ratio": 1.22, "min_move_pips": 0.25},
        "fast": {"period": 42, "alpha": 0.14, "beta": 0.78, "min_volatility_ratio": 1.12, "min_move_pips": 0.15},
        "loose": {"period": 30, "alpha": 0.18, "beta": 0.72, "min_volatility_ratio": 1.04, "min_move_pips": 0.08},
    },
    "hurst_regime_forecast": {
        "strict": {"period": 60, "trend_hurst": 0.62, "reversion_hurst": 0.38, "min_move_pips": 0.35, "min_reversal_pips": 0.12},
        "balanced": {"period": 56, "trend_hurst": 0.58, "reversion_hurst": 0.42, "min_move_pips": 0.25, "min_reversal_pips": 0.08},
        "fast": {"period": 48, "trend_hurst": 0.55, "reversion_hurst": 0.45, "min_move_pips": 0.15, "min_reversal_pips": 0.05},
        "loose": {"period": 36, "trend_hurst": 0.52, "reversion_hurst": 0.48, "min_move_pips": 0.08, "min_reversal_pips": 0.02},
    },
    "theil_sen_trend": {
        "strict": {"period": 60, "min_slope_pips": 0.10, "max_residual_to_move": 1.20},
        "balanced": {"period": 52, "min_slope_pips": 0.07, "max_residual_to_move": 1.60},
        "fast": {"period": 42, "min_slope_pips": 0.04, "max_residual_to_move": 2.10},
        "loose": {"period": 30, "min_slope_pips": 0.02, "max_residual_to_move": 3.00},
    },
    "vwap_deviation_reversion": {
        "strict": {"period": 60, "entry_atr": 1.50, "min_reversal_pips": 0.20, "min_volume_ratio": 1.10},
        "balanced": {"period": 52, "entry_atr": 1.20, "min_reversal_pips": 0.12, "min_volume_ratio": 1.00},
        "fast": {"period": 42, "entry_atr": 0.95, "min_reversal_pips": 0.07, "min_volume_ratio": 0.90},
        "loose": {"period": 30, "entry_atr": 0.70, "min_reversal_pips": 0.03, "min_volume_ratio": 0.75},
    },
    "ny_session_vwap_sell_reversion": {
        "strict": {"period": 60, "entry_atr": 1.50, "min_reversal_pips": 0.20, "min_volume_ratio": 1.10, "session_start_hour": 9, "session_end_hour": 17, "target_horizon_sec": 14400},
        "balanced": {"period": 52, "entry_atr": 1.20, "min_reversal_pips": 0.12, "min_volume_ratio": 1.00, "session_start_hour": 9, "session_end_hour": 17, "target_horizon_sec": 14400},
        "fast": {"period": 42, "entry_atr": 0.95, "min_reversal_pips": 0.07, "min_volume_ratio": 0.90, "session_start_hour": 9, "session_end_hour": 17, "target_horizon_sec": 14400},
        "loose": {"period": 30, "entry_atr": 0.70, "min_reversal_pips": 0.03, "min_volume_ratio": 0.75, "session_start_hour": 9, "session_end_hour": 17, "target_horizon_sec": 14400},
    },
    "inverse_correlation_veto": {
        "strict": {"min_sources": 4, "min_vote_magnitude": 1.50, "min_source_activity": 0.50},
        "balanced": {"min_sources": 3, "min_vote_magnitude": 1.00, "min_source_activity": 0.50},
        "fast": {"min_sources": 2, "min_vote_magnitude": 0.60, "min_source_activity": 0.25},
        "loose": {"min_sources": 1, "min_vote_magnitude": 0.25, "min_source_activity": 0.25},
    },
    "signal_combination_rules": {
        "strict": {"min_rule_size": 3, "min_train_support": 250.0, "min_holdout_support": 100.0, "min_lower_edge": 0.040, "min_membership": 0.75, "min_expected_net_pips": 0.25},
        "balanced": {"min_rule_size": 2, "min_train_support": 160.0, "min_holdout_support": 60.0, "min_lower_edge": 0.030, "min_membership": 0.65, "min_expected_net_pips": 0.15},
        "fast": {"min_rule_size": 2, "min_train_support": 100.0, "min_holdout_support": 40.0, "min_lower_edge": 0.020, "min_membership": 0.55, "min_expected_net_pips": 0.08},
        "loose": {"min_rule_size": 2, "min_train_support": 80.0, "min_holdout_support": 30.0, "min_lower_edge": 0.010, "min_membership": 0.45, "min_expected_net_pips": 0.03},
    },
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_contract_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def strategy_lab_code_version() -> str:
    try:
        digest = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    except OSError:
        digest = stable_contract_hash("oanda_practice_shadow_strategy_lab")
    return f"sha256:{digest}"


STRATEGY_LAB_CODE_VERSION = strategy_lab_code_version()


def lane_model_version(lane: LaneSpec) -> str:
    return "sha256:" + stable_contract_hash(
        {
            "strategy_lab": STRATEGY_LAB_CODE_VERSION,
            "lane_id": lane.lane_id,
            "family": lane.family,
            "profile": lane.profile,
            "parameters": lane.parameters,
        }
    )


def feature_contract_version(features: dict[str, Any]) -> str:
    return "sha256:" + stable_contract_hash(
        {
            "schema": "strategy_lab_feature_contract_v1",
            "keys": sorted(str(key) for key in features),
        }
    )


def canonical_forecast_id(
    lane: LaneSpec,
    instrument: str,
    data_cutoff_utc: str,
    model_version: str,
    feature_version: str,
) -> str:
    return "forecast_" + stable_contract_hash(
        {
            "lane_id": lane.lane_id,
            "instrument": instrument,
            "data_cutoff_utc": data_cutoff_utc,
            "model_version": model_version,
            "feature_version": feature_version,
        }
    )[:40]


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    # Windows readers can briefly hold the destination open while inspecting a
    # live snapshot.  Keep the write atomic, but allow enough time for those
    # short-lived handles to drain before treating the path as unhealthy.
    # Multiple read-only proof/monitor workers can briefly overlap on Windows.
    # Retain atomic replacement but tolerate a bounded multi-second reader
    # hold before surfacing a real producer failure.
    max_replace_attempts = 16
    for attempt in range(max_replace_attempts):
        try:
            temporary.replace(path)
            return
        except PermissionError:
            if attempt == max_replace_attempts - 1:
                raise
            time.sleep(min(0.5, 0.01 * (2**attempt)))


def log_line(path: Path, event: str, **fields: Any) -> None:
    count_only = bool(fields.pop("_count_only", False))
    payload = {"time": utc_now(), "event": event, **fields}
    path = Path(path)
    with LOG_LOCK:
        counts = LOG_EVENT_COUNTS.setdefault(path, {})
        counts[event] = counts.get(event, 0) + 1
        if event == "shadow_miss":
            miss_class = str(fields.get("miss_class") or "unknown")
            key = f"shadow_miss:{miss_class}"
            counts[key] = counts.get(key, 0) + 1
        if count_only:
            return
        line = json.dumps(payload, sort_keys=True, default=str)
        encoded_size = len((line + "\n").encode("utf-8"))
        handle = LOG_HANDLES.get(path)
        if handle is None or handle.closed:
            path.parent.mkdir(parents=True, exist_ok=True)
            handle = path.open("a", encoding="utf-8", buffering=1024 * 1024)
            LOG_HANDLES[path] = handle
            LOG_LAST_FLUSH[path] = time.monotonic()
            try:
                LOG_BYTES[path] = path.stat().st_size
            except OSError:
                LOG_BYTES[path] = 0
        try:
            max_bytes = int(
                os.environ.get("FOREX_LOG_MAX_BYTES", str(128 * 1024 * 1024))
            )
        except ValueError:
            max_bytes = 128 * 1024 * 1024
        if (
            max_bytes > 0
            and LOG_BYTES.get(path, 0) > 0
            and LOG_BYTES.get(path, 0) + encoded_size > max_bytes
            and time.monotonic() >= LOG_NEXT_ROTATION_ATTEMPT.get(path, 0.0)
        ):
            handle.flush()
            handle.close()
            sequence = LOG_ROTATION_SEQUENCE.get(path, 0) + 1
            LOG_ROTATION_SEQUENCE[path] = sequence
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
            rotated = path.with_name(
                f"{path.stem}.part_{stamp}_{sequence:04d}{path.suffix}"
            )
            try:
                os.replace(path, rotated)
            except OSError:
                handle = path.open(
                    "a",
                    encoding="utf-8",
                    buffering=1024 * 1024,
                )
                LOG_HANDLES[path] = handle
                LOG_LAST_FLUSH[path] = time.monotonic()
                try:
                    LOG_BYTES[path] = path.stat().st_size
                except OSError:
                    LOG_BYTES[path] = 0
                LOG_NEXT_ROTATION_ATTEMPT[path] = time.monotonic() + 60.0
            else:
                handle = path.open(
                    "a",
                    encoding="utf-8",
                    buffering=1024 * 1024,
                )
                LOG_HANDLES[path] = handle
                LOG_LAST_FLUSH[path] = time.monotonic()
                LOG_BYTES[path] = 0
                LOG_NEXT_ROTATION_ATTEMPT.pop(path, None)
        handle.write(line + "\n")
        LOG_BYTES[path] = LOG_BYTES.get(path, 0) + encoded_size
        now = time.monotonic()
        if (
            now - LOG_LAST_FLUSH.get(path, -math.inf) >= 0.25
            or event.startswith("practice_")
            or event
            in {
                "lab_start",
                "lab_stop",
                "lab_error",
                "lab_evaluation_cycle",
            }
        ):
            handle.flush()
            LOG_LAST_FLUSH[path] = now
        if os.environ.get("FOREX_LOG_STDOUT") == "1":
            print(line, flush=True)


def log_event_counts(path: Path) -> dict[str, int]:
    with LOG_LOCK:
        return dict(LOG_EVENT_COUNTS.get(Path(path), {}))


def close_log_handles() -> None:
    with LOG_LOCK:
        for handle in LOG_HANDLES.values():
            try:
                handle.flush()
                handle.close()
            except OSError:
                pass
        LOG_HANDLES.clear()
        LOG_LAST_FLUSH.clear()
        LOG_BYTES.clear()
        LOG_ROTATION_SEQUENCE.clear()
        LOG_NEXT_ROTATION_ATTEMPT.clear()
        LOG_EVENT_COUNTS.clear()


atexit.register(close_log_handles)


def account_lane_eligible(family: str, profile: str) -> bool:
    if family == "inverse_correlation_veto" or family in RESEARCH_SHADOW_FAMILIES:
        return False
    return not (profile == "loose" and family in COOLED_LOOSE_ACCOUNT_FAMILIES)


def default_lanes() -> list[LaneSpec]:
    lanes: list[LaneSpec] = []
    for family in DEFAULT_FAMILIES:
        for profile in PROFILES:
            parameters = {
                **GATE_PROFILES[profile],
                **FAMILY_PARAMETERS[family][profile],
                "account_eligible": account_lane_eligible(family, profile),
            }
            lanes.append(
                LaneSpec(
                    lane_id=f"{family}.{profile}",
                    family=family,
                    profile=profile,
                    parameters=parameters,
                )
            )
    ahl_parameters = {
        **GATE_PROFILES["balanced"],
        **FAMILY_PARAMETERS["ahl_multihorizon_trend"]["balanced"],
        "account_eligible": False,
    }
    lanes.append(
        LaneSpec(
            lane_id="ahl_multihorizon_trend.shadow",
            family="ahl_multihorizon_trend",
            profile="shadow",
            parameters=ahl_parameters,
        )
    )
    return lanes


def load_lanes(path: Path | None) -> list[LaneSpec]:
    if path is None:
        return default_lanes()
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("lanes") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        raise SystemExit("Lane config must be a JSON list or an object containing a lanes list.")
    lanes: list[LaneSpec] = []
    for row in rows:
        if not isinstance(row, dict):
            raise SystemExit("Each lane config entry must be an object.")
        family = str(row.get("family") or "")
        profile = str(row.get("profile") or "custom")
        if family not in FAMILIES:
            raise SystemExit(f"Unknown strategy family in lane config: {family}")
        parameters = {
            **GATE_PROFILES.get(profile, GATE_PROFILES["balanced"]),
            **FAMILY_PARAMETERS[family].get(profile, FAMILY_PARAMETERS[family]["balanced"]),
            "account_eligible": account_lane_eligible(family, profile),
            **dict(row.get("parameters") or {}),
        }
        lane_id = str(row.get("lane_id") or f"{family}.{profile}")
        lanes.append(LaneSpec(lane_id=lane_id, family=family, profile=profile, parameters=parameters))
    ids = [lane.lane_id for lane in lanes]
    if len(ids) != len(set(ids)):
        raise SystemExit("Lane ids must be unique.")
    return lanes


def candle_values(candles: list[dict[str, Any]], field: str) -> list[float]:
    return [safe_float((candle.get("mid") or {}).get(field)) for candle in candles]


def local_atr_pips(candles: list[dict[str, Any]], pip: float, period: int = 14) -> float:
    if len(candles) < period + 1:
        return 0.0
    ranges: list[float] = []
    previous_close: float | None = None
    for candle in candles:
        mid = candle.get("mid") or {}
        high = safe_float(mid.get("h"))
        low = safe_float(mid.get("l"))
        close = safe_float(mid.get("c"))
        true_range = high - low if previous_close is None else max(high - low, abs(high - previous_close), abs(low - previous_close))
        ranges.append(max(0.0, true_range))
        previous_close = close
    return statistics.fmean(ranges[-period:]) / pip


def classify_volatility_regime(atr_pips: Any, spread_pips: Any) -> str:
    atr = safe_float(atr_pips)
    spread = max(0.05, safe_float(spread_pips))
    if atr <= 0.0:
        return "unknown"
    movement_to_cost = atr / spread
    if movement_to_cost <= 3.0:
        return "compressed"
    if movement_to_cost >= 9.0:
        return "expanded"
    return "normal"


def resampled_series(
    candles: list[dict[str, Any]],
    minutes: int,
    *,
    source_minutes: int = 5,
) -> tuple[list[float], str]:
    expected = max(1, minutes // max(1, source_minutes))
    buckets: dict[int, list[dict[str, Any]]] = {}
    for candle in candles:
        parsed = parse_rfc3339(str(candle.get("time") or ""))
        if parsed is None:
            continue
        key = int(parsed.timestamp()) // (minutes * 60)
        buckets.setdefault(key, []).append(candle)
    output: list[float] = []
    complete_keys: list[int] = []
    for key in sorted(buckets):
        bucket = sorted(buckets[key], key=lambda item: str(item.get("time") or ""))
        if len(bucket) == expected:
            output.append(safe_float((bucket[-1].get("mid") or {}).get("c")))
            complete_keys.append(key)
    return output, str(complete_keys[-1]) if complete_keys else ""


def resampled_closes(
    candles: list[dict[str, Any]],
    minutes: int,
    *,
    source_minutes: int = 5,
) -> list[float]:
    return resampled_series(
        candles,
        minutes,
        source_minutes=source_minutes,
    )[0]


def aggregate_complete_candles(
    candles: list[dict[str, Any]],
    target_seconds: int,
    source_seconds: int,
) -> list[dict[str, Any]]:
    """Aggregate complete OANDA candles without filling missing source bars."""

    target = max(1, int(target_seconds))
    source = max(1, int(source_seconds))
    if target < source or target % source:
        return []
    expected = target // source
    buckets: dict[int, dict[int, dict[str, Any]]] = {}
    for candle in candles:
        parsed = parse_rfc3339(str(candle.get("time") or ""))
        if parsed is None or not bool(candle.get("complete", True)):
            continue
        source_key = int(parsed.timestamp()) // source
        target_key = int(parsed.timestamp()) // target
        buckets.setdefault(target_key, {})[source_key] = candle
    output: list[dict[str, Any]] = []
    for target_key in sorted(buckets):
        ordered = [buckets[target_key][key] for key in sorted(buckets[target_key])]
        if len(ordered) != expected:
            continue
        aggregated: dict[str, Any] = {
            "time": datetime.fromtimestamp(
                target_key * target,
                timezone.utc,
            ).isoformat().replace("+00:00", "Z"),
            "complete": True,
            "volume": sum(safe_float(row.get("volume")) for row in ordered),
        }
        for side in ("mid", "bid", "ask"):
            values = [row.get(side) for row in ordered]
            if not all(isinstance(value, dict) for value in values):
                continue
            aggregated[side] = {
                "o": safe_float(values[0].get("o")),
                "h": max(safe_float(value.get("h")) for value in values),
                "l": min(safe_float(value.get("l")) for value in values),
                "c": safe_float(values[-1].get("c")),
            }
        if "mid" in aggregated:
            output.append(aggregated)
    return output


def build_timeframe_feature_views(
    instrument: str,
    candle_sets: dict[str, list[dict[str, Any]]],
    pip_size: float | None = None,
    *,
    cache: dict[
        tuple[str, str],
        tuple[str, dict[str, Any]],
    ]
    | None = None,
    primary_features: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    """Recreate the primary-timeframe feature semantics used in panel training."""

    m1 = candle_sets.get("M1") or []
    m5 = candle_sets.get("M5") or []
    h1 = candle_sets.get("H1") or []
    primaries = {
        "M1": m1,
        "M5": m5,
        "M10": aggregate_complete_candles(m5, 600, 300),
        "M15": aggregate_complete_candles(m5, 900, 300),
        "M30": aggregate_complete_candles(m5, 1800, 300),
        "H1": h1,
        "H2": aggregate_complete_candles(h1, 7200, 3600),
        "H3": aggregate_complete_candles(h1, 10800, 3600),
        "H4": aggregate_complete_candles(h1, 14400, 3600),
    }
    seconds = {
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
    output: dict[str, dict[str, Any]] = {}
    for timeframe, primary in primaries.items():
        identity = "|".join(
            (
                str(len(primary)),
                str((primary[-1] if primary else {}).get("time") or ""),
                str(len(m5)),
                str((m5[-1] if m5 else {}).get("time") or ""),
                f"{safe_float(pip_size, infer_pip_size(instrument)):.12g}",
            )
        )
        cache_key = (instrument, timeframe)
        cached = None if cache is None else cache.get(cache_key)
        if cached is not None and cached[0] == identity:
            output[timeframe] = cached[1]
            continue
        if timeframe == "M1" and primary_features is not None:
            features = dict(primary_features)
        else:
            features, _ = build_features(
                instrument,
                {"M1": primary, "M5": m5},
                pip_size,
                include_ma_grid=False,
            )
        if features is None:
            continue
        features["input_timeframe"] = timeframe
        features["input_timeframe_seconds"] = seconds[timeframe]
        output[timeframe] = features
        if cache is not None:
            cache[cache_key] = (identity, features)
    return output


def candle_spreads_pips(candles: list[dict[str, Any]], pip: float) -> list[float]:
    spreads: list[float] = []
    if pip <= 0.0:
        return spreads
    for candle in candles:
        bid = candle.get("bid") or {}
        ask = candle.get("ask") or {}
        bid_close = safe_float(bid.get("c"))
        ask_close = safe_float(ask.get("c"))
        if bid_close > 0.0 and ask_close > 0.0 and ask_close >= bid_close:
            spreads.append((ask_close - bid_close) / pip)
    return spreads


def recent_ratio(values: list[float], lookback: int, default: float = 1.0) -> float:
    if len(values) < lookback + 1:
        return default
    baseline = statistics.median(values[-lookback - 1 : -1])
    if baseline <= 0.0:
        return default
    return safe_float(values[-1]) / baseline


def parsed_complete_closes(
    candles: list[dict[str, Any]],
    source_seconds: int,
) -> list[tuple[int, int, float, str]]:
    """Parse a source candle stream once for every derived MA timeframe."""

    parsed_rows: list[tuple[int, int, float, str]] = []
    source = max(1, int(source_seconds))
    for candle in candles:
        parsed = parse_rfc3339(str(candle.get("time") or ""))
        if parsed is None or not bool(candle.get("complete", True)):
            continue
        timestamp = int(parsed.timestamp())
        parsed_rows.append(
            (
                timestamp,
                timestamp // source,
                safe_float((candle.get("mid") or {}).get("c")),
                str(candle.get("time") or ""),
            )
        )
    return parsed_rows


def aggregate_close_series(
    parsed_rows: list[tuple[int, int, float, str]],
    target_seconds: int,
    source_seconds: int,
) -> tuple[list[float], str]:
    """Aggregate close-only series while rejecting incomplete source buckets."""

    target = max(1, int(target_seconds))
    source = max(1, int(source_seconds))
    if target < source or target % source:
        return [], ""
    expected = target // source
    buckets: dict[int, dict[int, float]] = {}
    for timestamp, source_key, close, _ in parsed_rows:
        buckets.setdefault(timestamp // target, {})[source_key] = close
    values: list[float] = []
    complete_keys: list[int] = []
    for target_key in sorted(buckets):
        source_values = buckets[target_key]
        if len(source_values) != expected:
            continue
        values.append(source_values[max(source_values)])
        complete_keys.append(target_key)
    origin = (
        datetime.fromtimestamp(
            complete_keys[-1] * target,
            timezone.utc,
        ).isoformat().replace("+00:00", "Z")
        if complete_keys
        else ""
    )
    return values[-512:], origin


def build_ma_series_by_timeframe(
    candle_sets: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, list[float]], dict[str, str]]:
    """Build all live MA-grid series with one parse pass per source stream."""

    parsed_m1 = parsed_complete_closes(candle_sets.get("M1") or [], 60)
    parsed_h1 = parsed_complete_closes(candle_sets.get("H1") or [], 3600)
    series: dict[str, list[float]] = {}
    origins: dict[str, str] = {}

    def add_source(timeframe: str, rows: list[tuple[int, int, float, str]]) -> None:
        if not rows:
            return
        series[timeframe] = [row[2] for row in rows[-512:]]
        origins[timeframe] = rows[-1][3]

    def add_derived(
        timeframe: str,
        rows: list[tuple[int, int, float, str]],
        target_seconds: int,
        source_seconds: int,
    ) -> None:
        values, origin = aggregate_close_series(
            rows,
            target_seconds,
            source_seconds,
        )
        if values:
            series[timeframe] = values
            origins[timeframe] = origin

    add_source("M1", parsed_m1)
    for timeframe, seconds in (
        ("M2", 120),
        ("M3", 180),
        ("M4", 240),
        ("M5", 300),
        ("M6", 360),
        ("M7", 420),
        ("M8", 480),
        ("M9", 540),
        ("M10", 600),
        ("M12", 720),
        ("M15", 900),
        ("M20", 1200),
        ("M30", 1800),
        ("M45", 2700),
    ):
        add_derived(timeframe, parsed_m1, seconds, 60)
    add_source("H1", parsed_h1)
    for timeframe, seconds in (
        ("H2", 7200),
        ("H3", 10800),
        ("H4", 14400),
        ("H6", 21600),
        ("H8", 28800),
        ("H12", 43200),
        ("D1", 86400),
    ):
        add_derived(timeframe, parsed_h1, seconds, 3600)
    return series, origins


def build_features(
    instrument: str,
    candle_sets: dict[str, list[dict[str, Any]]],
    pip_size: float | None = None,
    *,
    include_ma_grid: bool = True,
) -> tuple[dict[str, Any] | None, str]:
    m1 = candle_sets.get("M1") or []
    m5 = candle_sets.get("M5") or []
    h1 = candle_sets.get("H1") or []
    d1 = candle_sets.get("D") or []
    if len(m1) < 60 or len(m5) < 40:
        return None, "insufficient_candles"
    pip = safe_float(pip_size, infer_pip_size(instrument))
    closes = candle_closes(m1)
    m5_closes = candle_closes(m5)
    h1_closes = candle_closes(h1) if len(h1) >= 8 else resampled_closes(m5, 60)
    d1_closes = candle_closes(d1)
    m10_closes, m10_origin = resampled_series(m5, 10)
    m15_closes, m15_origin = resampled_series(m5, 15)
    m30_closes, m30_origin = resampled_series(m5, 30)
    if len(h1) >= 8:
        h1_origin = str(h1[-1].get("time") or "")
        h2_closes, h2_origin = resampled_series(h1, 120, source_minutes=60)
        h3_closes, h3_origin = resampled_series(h1, 180, source_minutes=60)
        h4_closes, h4_origin = resampled_series(h1, 240, source_minutes=60)
    else:
        _, h1_origin = resampled_series(m5, 60)
        h2_closes, h2_origin = resampled_series(m5, 120)
        h3_closes, h3_origin = resampled_series(m5, 180)
        h4_closes, h4_origin = resampled_series(m5, 240)
    opens = candle_values(m1, "o")
    highs = candle_values(m1, "h")
    lows = candle_values(m1, "l")
    volumes = [safe_float(candle.get("volume")) for candle in m1]
    spread_history = candle_spreads_pips(m1, pip)
    m5_spread_history = candle_spreads_pips(m5, pip)
    volume_ratio_12 = recent_ratio(volumes, 12)
    volume_ratio_30 = recent_ratio(volumes, 30)
    spread_ratio_12 = recent_ratio(spread_history, 12)
    ma_series, ma_origins = (
        build_ma_series_by_timeframe(candle_sets)
        if include_ma_grid
        else ({}, {})
    )
    window = closes[-20:]
    low = min(window)
    high = max(window)
    last = closes[-1]
    features = {
        "instrument": instrument,
        "pip": pip,
        "candle_time": str(m1[-1].get("time") or ""),
        "closes": closes,
        "opens": opens,
        "highs": highs,
        "lows": lows,
        "volumes": volumes,
        "volume_ratio_12": volume_ratio_12,
        "volume_ratio_30": volume_ratio_30,
        "current_volume": safe_float(volumes[-1]) if volumes else 0.0,
        "spread_history_pips": spread_history,
        "m5_spread_history_pips": m5_spread_history,
        "current_candle_spread_pips": safe_float(spread_history[-1]) if spread_history else 0.0,
        "median_spread_12_pips": statistics.median(spread_history[-13:-1]) if len(spread_history) >= 13 else 0.0,
        "spread_ratio_12": spread_ratio_12,
        "spread_drop_3_pips": (spread_history[-4] - spread_history[-1]) if len(spread_history) >= 4 else 0.0,
        "m5_closes": m5_closes,
        "m10_closes": m10_closes,
        "m15_closes": m15_closes,
        "m30_closes": m30_closes,
        "h1_closes": h1_closes,
        "h2_closes": h2_closes,
        "h3_closes": h3_closes,
        "h4_closes": h4_closes,
        "d1_closes": d1_closes,
        "d1_candles": d1,
        "d1_origin": str(d1[-1].get("time") or "") if d1 else "",
        "series_origins": {
            "M1": str(m1[-1].get("time") or ""),
            "M5": str(m5[-1].get("time") or ""),
            "M10": m10_origin,
            "M15": m15_origin,
            "M30": m30_origin,
            "H1": h1_origin,
            "H2": h2_origin,
            "H3": h3_origin,
            "H4": h4_origin,
        },
        "ma_series_by_timeframe": ma_series,
        "ma_series_origins": ma_origins,
        "last": last,
        "r1_pips": (last - closes[-2]) / pip,
        "r3_pips": (last - closes[-4]) / pip,
        "r5_pips": (last - closes[-6]) / pip,
        "m5_r1_pips": (m5_closes[-1] - m5_closes[-2]) / pip,
        "m5_r3_pips": (m5_closes[-1] - m5_closes[-4]) / pip,
        "pos20": 0.5 if math.isclose(high, low) else (last - low) / (high - low),
        "m1_atr14_pips": local_atr_pips(m1, pip),
        "m5_atr14_pips": local_atr_pips(m5, pip),
    }
    return features, ""


def load_order_position_book_features(
    path: Path | None,
    max_age_sec: float,
) -> dict[str, dict[str, Any]]:
    if path is None or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    generated_epoch = safe_float(payload.get("generated_epoch"))
    if generated_epoch <= 0.0 or time.time() - generated_epoch > max_age_sec:
        return {}
    rows = payload.get("instruments") or {}
    return {
        str(instrument): dict(row)
        for instrument, row in rows.items()
        if isinstance(row, dict)
    }


def merge_microstructure_metadata(
    pricing: dict[str, dict[str, Any]],
    books: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    instruments = set(pricing) | set(books)
    return {
        instrument: {
            **dict(pricing.get(instrument) or {}),
            **dict(books.get(instrument) or {}),
        }
        for instrument in instruments
    }


def augment_market_microstructure_features(
    feature_cache: dict[str, dict[str, Any]],
    prices: dict[str, Any],
    metadata: dict[str, dict[str, Any]] | None,
) -> None:
    for instrument, features in feature_cache.items():
        quote = prices.get(instrument)
        row = (metadata or {}).get(instrument) or {}
        pip = max(1e-12, safe_float(features.get("pip"), infer_pip_size(instrument)))
        bid = safe_float(getattr(quote, "bid", 0.0))
        ask = safe_float(getattr(quote, "ask", 0.0))
        if bid > 0.0 and ask >= bid:
            features["live_spread_pips"] = (ask - bid) / pip
        for name in MICROSTRUCTURE_SCALAR_KEYS:
            if row.get(name) is not None:
                features[name] = safe_float(row.get(name))
        bid_total = max(0.0, safe_float(features.get("bid_total_liquidity")))
        ask_total = max(0.0, safe_float(features.get("ask_total_liquidity")))
        bid_top = max(0.0, safe_float(features.get("bid_top_liquidity")))
        ask_top = max(0.0, safe_float(features.get("ask_top_liquidity")))
        features["depth_total_liquidity"] = bid_total + ask_total
        features["depth_log_total_liquidity"] = math.log1p(bid_total + ask_total)
        features["depth_top_imbalance"] = (
            (bid_top - ask_top) / (bid_top + ask_top)
            if bid_top + ask_top > 0.0
            else 0.0
        )
        microprice = safe_float(features.get("microprice"))
        mid = 0.5 * (bid + ask) if bid > 0.0 and ask >= bid else safe_float(features.get("last"))
        features["microprice_offset_pips"] = (
            (microprice - mid) / pip if microprice > 0.0 and mid > 0.0 else 0.0
        )
        received = safe_float(row.get("received_monotonic"))
        features["quote_receive_age_sec"] = (
            max(0.0, time.monotonic() - received) if received > 0.0 else 0.0
        )
        features["order_book_available"] = float(
            any(str(name).startswith("order_book_") and row.get(name) is not None for name in row)
        )
        features["position_book_available"] = float(
            any(str(name).startswith("position_book_") and row.get(name) is not None for name in row)
        )


def write_live_model_feature_snapshot(
    path: Path | None,
    cycle_id: str,
    feature_cache: dict[str, dict[str, Any]],
    prices: dict[str, Any],
    metadata: dict[str, dict[str, Any]] | None,
    timeframe_feature_cache: dict[str, dict[str, dict[str, Any]]] | None = None,
    *,
    observation_archive_root: Path | None = None,
    observation_clocks: dict[str, Any] | None = None,
) -> int:
    if path is None:
        return 0
    generated_epoch = time.time()
    rows: dict[str, dict[str, Any]] = {}
    quote_exclusions: list[dict[str, Any]] = []
    series_names = {
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
    for instrument, features in sorted(feature_cache.items()):
        quote = prices.get(instrument)
        rejection_reason = outcome_quote_rejection_reason(quote)
        if rejection_reason:
            quote_exclusions.append(
                {
                    "instrument": instrument,
                    "reason": rejection_reason,
                    "quote_time": "" if quote is None else str(getattr(quote, "time", "") or ""),
                    "tradeable": None if quote is None else bool(getattr(quote, "tradeable", True)),
                }
            )
            continue
        scalars: dict[str, Any] = {}
        for name, value in features.items():
            if isinstance(value, bool) or value is None or isinstance(value, str):
                scalars[name] = value
            elif isinstance(value, (int, float, np.integer, np.floating)):
                numeric = safe_float(value, math.nan)
                scalars[name] = numeric if math.isfinite(numeric) else None
        series = {
            timeframe: [safe_float(value) for value in (features.get(source_name) or [])[-512:]]
            for timeframe, source_name in series_names.items()
            if features.get(source_name)
        }
        structural_views = {
            timeframe: structural_feature_view(timeframe_features)
            for timeframe, timeframe_features in (
                (timeframe_feature_cache or {}).get(instrument) or {}
            ).items()
        }
        context_views = filter_context_views(structural_views)
        structural_series = {
            timeframe: {
                "bar_start_utc": str(values.get("candle_time") or ""),
                "bar_start_times_utc": list(
                    (values.get("candle_times") or [])[-512:]
                ),
                "open": [
                    safe_float(value) for value in (values.get("opens") or [])[-512:]
                ],
                "high": [
                    safe_float(value) for value in (values.get("highs") or [])[-512:]
                ],
                "low": [
                    safe_float(value) for value in (values.get("lows") or [])[-512:]
                ],
                "close": [
                    safe_float(value) for value in (values.get("closes") or [])[-512:]
                ],
                "tick_activity": [
                    safe_float(value) for value in (values.get("volumes") or [])[-512:]
                ],
                "historical_spread_pips": [
                    safe_float(value)
                    for value in (values.get("spread_history_pips") or [])[-512:]
                ],
            }
            for timeframe, values in context_views.items()
            if values.get("closes")
        }
        rows[instrument] = {
            "feature_origin_utc": str(features.get("candle_time") or ""),
            "quote": {
                "bid": safe_float(quote.bid),
                "ask": safe_float(quote.ask),
                "time": str(quote.time or ""),
                "source": str(getattr(quote, "source", "") or ""),
            },
            "features": scalars,
            "timeframe_features": {
                timeframe: {
                    name: (
                        value
                        if isinstance(value, (bool, str)) or value is None
                        else safe_float(value, math.nan)
                    )
                    for name, value in timeframe_features.items()
                    if isinstance(
                        value,
                        (bool, str, int, float, np.integer, np.floating),
                    )
                    and (
                        not isinstance(value, (int, float, np.integer, np.floating))
                        or math.isfinite(safe_float(value, math.nan))
                    )
                }
                for timeframe, timeframe_features in sorted(structural_views.items())
            },
            "unified_forecast_features": flatten_context_views(context_views),
            "structural_series": structural_series,
            "series": series,
            "microstructure": dict((metadata or {}).get(instrument) or {}),
        }
    payload = {
            "schema_version": 1,
            "snapshot_id": cycle_id,
            "generated_epoch": generated_epoch,
            "generated_utc": datetime.fromtimestamp(generated_epoch, timezone.utc).isoformat(),
            "instrument_count": len(rows),
            "instruments": rows,
            "coverage": {
                "expected_feature_instrument_count": len(feature_cache),
                "quote_input_count": len(prices),
                "accepted_instrument_count": len(rows),
                "excluded_instrument_count": len(quote_exclusions),
                "quote_exclusions": quote_exclusions,
                "fail_closed_on_invalid_quote": True,
            },
            "contract": {
                "feature_values_observable_at_generation": True,
                "historical_outcomes_included": False,
                "quotes_are_executable_bid_ask": True,
                "series_tail_limit": 512,
                "structural_series_fields": [
                    "open",
                    "high",
                    "low",
                    "close",
                    "tick_activity",
                    "historical_spread_pips",
                    "bar_start_times_utc",
                ],
                "timeframe_features_match_training_primary_semantics": True,
                "structural_features_exclude_execution_microstructure": True,
                "intrahour_forecast": intrahour_contract_payload(),
            },
        }
    clocks = observation_clocks or {}
    observation_pairs: dict[str, Any] = {}
    exclusion_by_pair = {row["instrument"]: row["reason"] for row in quote_exclusions}
    for instrument, features in sorted(feature_cache.items()):
        quote = prices.get(instrument)
        earlier_quote_utc = str((clocks.get("quote_observed_utc_by_instrument") or {}).get(instrument) or "")
        component_clocks = {
            "quote_feature_source_utc": earlier_quote_utc,
            "quote_feature_capture_utc": str(clocks.get("quote_feature_capture_utc") or ""),
            "timeframe_capture_utc": str(clocks.get("timeframe_observed_utc") or ""),
            "primary_capture_utc": str(clocks.get("primary_observed_utc") or ""),
            "primary_calculated_utc": str((clocks.get("primary_calculated_utc_by_instrument") or {}).get(instrument) or ""),
            "timeframe_view_retrieved_utc": str((clocks.get("timeframe_view_retrieved_utc_by_instrument") or {}).get(instrument) or ""),
            "order_book_source_utc": str(((metadata or {}).get(instrument) or {}).get("order_book_time_utc") or ""),
            "position_book_source_utc": str(((metadata or {}).get(instrument) or {}).get("position_book_time_utc") or ""),
        }
        def captured_clock(key: str) -> dict[str, Any]:
            observed = str(clocks.get(key) or "")
            return {"observed_utc": observed, "clock_basis": "producer_capture" if observed else "unknown", "component_clocks": component_clocks}
        groups = {
            "primary": capture_feature_group(features, input_timeframe="M1", clock=captured_clock("primary_observed_utc")),
            "microstructure": capture_feature_group(
                dict((metadata or {}).get(instrument) or {}), input_timeframe="QUOTE",
                clock={"observed_utc": earlier_quote_utc, "bar_complete_utc": earlier_quote_utc, "clock_basis": "source_quote_before_enrichment" if earlier_quote_utc else "unknown", "component_clocks": component_clocks},
            ),
        }
        groups.update({
            f"timeframe:{timeframe}": capture_feature_group(values, input_timeframe=timeframe, clock=captured_clock("timeframe_observed_utc"))
            for timeframe, values in sorted(((timeframe_feature_cache or {}).get(instrument) or {}).items())
        })
        observation_pairs[instrument] = {
            "quote": {"bid": None if quote is None else normalize_observation_json(quote.bid), "ask": None if quote is None else normalize_observation_json(quote.ask), "time": "" if quote is None else str(quote.time or ""), "source": "" if quote is None else str(getattr(quote, "source", "") or ""), "tradeable": None if quote is None else bool(getattr(quote, "tradeable", True))},
            "groups": groups,
            "coverage": {"quote_accepted": instrument in rows, "quote_exclusion_reason": exclusion_by_pair.get(instrument), "primary_scalar_count": len(groups["primary"]["values"]), "timeframe_group_count": len(groups) - 2},
        }
    payload["observation_source"] = {
        "producer_id": "oanda_practice_shadow_strategy_lab.write_live_model_feature_snapshot",
        "producer_contract_id": OBSERVATION_PRODUCER_CONTRACT,
        "producer_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "observation_implementation_sha256": observation_source_sha256(),
        "structural_contract": intrahour_contract_payload(),
    }
    payload["observation_inputs"] = {"schema_version": "feature_observation_inputs_v1", "instruments": observation_pairs}
    payload["coverage"]["expected_feature_instruments"] = sorted(feature_cache)
    # This observation history does not depend on model-worker liveness or its
    # permissive forecast freshness limit. Failure is visible to the producer.
    archive_observation_snapshot(payload, observation_archive_root or path.parent / "feature_observations_v1")
    atomic_json(path, payload)
    return len(rows)


def _normalized_pair_return(features: dict[str, Any], key: str) -> float:
    atr = max(0.1, safe_float(features.get("m1_atr14_pips")))
    value = safe_float(features.get(key)) / atr
    return max(-6.0, min(6.0, value))


def _breadth(values: list[float]) -> float:
    if not values:
        return 0.0
    signed = sum(1.0 if value > 0.0 else -1.0 if value < 0.0 else 0.0 for value in values)
    return signed / len(values)


def _percentile_rank(value: float, values: list[float]) -> float:
    if not values:
        return 0.5
    below = sum(item < value for item in values)
    equal = sum(math.isclose(item, value, rel_tol=1e-12, abs_tol=1e-12) for item in values)
    return (below + 0.5 * equal) / len(values)


def augment_cross_sectional_features(feature_cache: dict[str, dict[str, Any]]) -> None:
    """Add leave-one-pair-out currency strength and residual features."""
    contributions: dict[str, dict[str, list[tuple[str, float]]]] = {
        "r1_pips": {},
        "r3_pips": {},
        "r5_pips": {},
    }
    normalized: dict[str, dict[str, float]] = {}
    for instrument, features in feature_cache.items():
        parts = instrument.split("_")
        if len(parts) != 2:
            continue
        base, quote = parts
        normalized[instrument] = {}
        for key in contributions:
            value = _normalized_pair_return(features, key)
            normalized[instrument][key] = value
            contributions[key].setdefault(base, []).append((instrument, value))
            contributions[key].setdefault(quote, []).append((instrument, -value))

    rank_values = {
        key: [values[key] for values in normalized.values()]
        for key in contributions
    }

    for instrument, features in feature_cache.items():
        features.update(
            {
                "cross_sample_count": 0,
                "cross_strength_r1": None,
                "cross_strength_r3": None,
                "cross_strength_r5": None,
                "cross_breadth_r1": None,
                "cross_breadth_r3": None,
                "pair_norm_r1": _normalized_pair_return(features, "r1_pips"),
                "pair_norm_r3": _normalized_pair_return(features, "r3_pips"),
                "pair_norm_r5": _normalized_pair_return(features, "r5_pips"),
                "cross_pair_count": len(normalized),
                "pair_rank_r3": None,
                "pair_rank_r5": None,
                "relative_residual_r3": None,
                "relative_residual_r5": None,
            }
        )
        parts = instrument.split("_")
        if len(parts) != 2 or instrument not in normalized:
            continue
        base, quote = parts
        leave_one_out: dict[str, tuple[list[float], list[float]]] = {}
        for key in contributions:
            base_values = [value for pair, value in contributions[key].get(base, []) if pair != instrument]
            quote_values = [value for pair, value in contributions[key].get(quote, []) if pair != instrument]
            leave_one_out[key] = (base_values, quote_values)
        sample_count = min(
            len(values)
            for pair_values in leave_one_out.values()
            for values in pair_values
        )
        if sample_count <= 0:
            continue
        base_r3, quote_r3 = leave_one_out["r3_pips"]
        base_r1, quote_r1 = leave_one_out["r1_pips"]
        base_r5, quote_r5 = leave_one_out["r5_pips"]
        strength_r1 = 0.5 * (statistics.median(base_r1) - statistics.median(quote_r1))
        strength_r3 = 0.5 * (statistics.median(base_r3) - statistics.median(quote_r3))
        strength_r5 = 0.5 * (statistics.median(base_r5) - statistics.median(quote_r5))
        breadth_r1 = 0.5 * (_breadth(base_r1) - _breadth(quote_r1))
        breadth_r3 = 0.5 * (_breadth(base_r3) - _breadth(quote_r3))
        features.update(
            {
                "pair_rank_r3": _percentile_rank(normalized[instrument]["r3_pips"], rank_values["r3_pips"]),
                "pair_rank_r5": _percentile_rank(normalized[instrument]["r5_pips"], rank_values["r5_pips"]),
                "cross_sample_count": sample_count,
                "cross_strength_r1": strength_r1,
                "cross_strength_r3": strength_r3,
                "cross_strength_r5": strength_r5,
                "cross_breadth_r1": breadth_r1,
                "cross_breadth_r3": breadth_r3,
                "relative_residual_r3": normalized[instrument]["r3_pips"] - strength_r3,
                "relative_residual_r5": normalized[instrument]["r5_pips"] - strength_r5,
            }
        )


def _supervised_contiguous_candle_data(candles: list[dict[str, Any]], pip: float) -> dict[str, Any] | None:
    if len(candles) < 60 or pip <= 0.0:
        return None
    mid = np.asarray([safe_float((row.get("mid") or {}).get("c")) for row in candles], dtype=float)
    high = np.asarray([safe_float((row.get("mid") or {}).get("h")) for row in candles], dtype=float)
    low = np.asarray([safe_float((row.get("mid") or {}).get("l")) for row in candles], dtype=float)
    bid = np.asarray([safe_float((row.get("bid") or {}).get("c")) for row in candles], dtype=float)
    ask = np.asarray([safe_float((row.get("ask") or {}).get("c")) for row in candles], dtype=float)
    if not all(np.all(np.isfinite(values)) and np.all(values > 0.0) for values in (mid, high, low, bid, ask)):
        return None
    ema5 = np.asarray(ema_series(mid.tolist(), 5), dtype=float)
    ema13 = np.asarray(ema_series(mid.tolist(), 13), dtype=float)
    rsi = rsi_series(mid.tolist())
    returns_pips = np.diff(mid, prepend=mid[0]) / pip
    true_range = np.zeros(len(mid), dtype=float)
    true_range[0] = high[0] - low[0]
    for index in range(1, len(mid)):
        true_range[index] = max(
            high[index] - low[index],
            abs(high[index] - mid[index - 1]),
            abs(low[index] - mid[index - 1]),
        )
    atr_pips = np.full(len(mid), np.nan, dtype=float)
    for index in range(13, len(mid)):
        atr_pips[index] = float(np.mean(true_range[index - 13 : index + 1]) / pip)

    def vector(index: int) -> list[float] | None:
        atr = atr_pips[index]
        if not math.isfinite(atr) or atr <= 0.0 or index < 36:
            return None
        window = mid[index - 19 : index + 1]
        window_low = float(np.min(window))
        window_high = float(np.max(window))
        position = 0.0 if math.isclose(window_low, window_high) else 2.0 * (mid[index] - window_low) / (window_high - window_low) - 1.0
        long_vol = max(0.05, float(np.std(returns_pips[index - 11 : index + 1])))
        short_vol = float(np.std(returns_pips[index - 2 : index + 1]))
        rsi_value = rsi[index]
        return [
            float((mid[index] - mid[index - 1]) / pip / atr),
            float((mid[index] - mid[index - 3]) / pip / atr),
            float((mid[index] - mid[index - 6]) / pip / atr),
            float((ema5[index] - ema13[index]) / pip / atr),
            0.0 if rsi_value is None else float((rsi_value - 50.0) / 50.0),
            float(position),
            float(short_vol / long_vol),
            float((ask[index] - bid[index]) / pip / atr),
        ]

    training_x: list[list[float]] = []
    training_y: list[list[float]] = []
    training_times: list[str] = []
    for index in range(36, len(mid) - 1):
        row = vector(index)
        atr = atr_pips[index]
        if row is None or not math.isfinite(atr) or atr <= 0.0:
            continue
        training_x.append(row)
        training_y.append(
            [
                float((bid[index + 1] - ask[index]) / pip / atr),
                float((bid[index] - ask[index + 1]) / pip / atr),
            ]
        )
        training_times.append(str(candles[index].get("time") or ""))
    current_x = vector(len(mid) - 1)
    if current_x is None:
        return None
    return {
        "x": training_x,
        "y": training_y,
        "times": training_times,
        "current_x": current_x,
        "current_atr_pips": float(atr_pips[-1]),
        "model_candle_time": str(candles[-1].get("time") or ""),
    }


def _supervised_candle_data(candles: list[dict[str, Any]], pip: float) -> dict[str, Any] | None:
    return supervised_candle_data_v2(candles, pip, _supervised_contiguous_candle_data)


def _fit_ridge(x: np.ndarray, y: np.ndarray, alpha: float = 8.0) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    mean = np.mean(x, axis=0)
    scale = np.std(x, axis=0)
    scale = np.where(scale < 1e-6, 1.0, scale)
    design = np.column_stack((np.ones(len(x)), (x - mean) / scale))
    penalty = np.eye(design.shape[1], dtype=float) * alpha
    penalty[0, 0] = 0.0
    matrix = design.T @ design + penalty
    target = design.T @ y
    try:
        coefficients = np.linalg.solve(matrix, target)
    except np.linalg.LinAlgError:
        coefficients = np.linalg.pinv(matrix) @ target
    return mean, scale, coefficients


def _predict_ridge(model: tuple[np.ndarray, np.ndarray, np.ndarray], x: np.ndarray) -> np.ndarray:
    mean, scale, coefficients = model
    design = np.column_stack((np.ones(len(x)), (x - mean) / scale))
    return design @ coefficients


def augment_supervised_return_features(
    feature_cache: dict[str, dict[str, Any]],
    candle_sets: dict[str, dict[str, list[dict[str, Any]]]],
) -> None:
    defaults = {
        "supervised_ready": False,
        "supervised_data_contract": SUPERVISED_DATA_CONTRACT,
        "supervised_direction": None,
        "supervised_expected_net_pips": None,
        "supervised_long_net_pips": None,
        "supervised_short_net_pips": None,
        "supervised_rank_percentile": 0.0,
        "supervised_model_rows": 0,
        "supervised_validation_rows": 0,
        "supervised_validation_hit_rate": 0.0,
        "supervised_validation_avg_norm": 0.0,
        "supervised_model_candle_time": "",
        "supervised_model_decision_time": "",
        "supervised_model_asof_time": "",
        "supervised_data_quality": {},
        "supervised_purged_training_rows": 0,
        "supervised_withheld_reason": "insufficient_mature_training",
        "supervised_top_instrument": "",
        "supervised_top_expected_net_pips": 0.0,
    }
    for features in feature_cache.values():
        features.update(defaults)

    cache_key = tuple(
        (instrument, supervised_input_fingerprint(
            (candle_sets.get(instrument) or {}).get("M5") or [],
            feature_cache[instrument].get("pip"),
        ))
        for instrument in sorted(feature_cache)
    )

    def apply_cached(payload: dict[str, Any]) -> None:
        meta = payload.get("meta") or {}
        for instrument, reason in (payload.get("rejections") or {}).items():
            if instrument in feature_cache:
                feature_cache[instrument]["supervised_withheld_reason"] = reason
        for instrument, score in (payload.get("scores") or {}).items():
            if instrument in feature_cache:
                feature_cache[instrument].update(meta)
                feature_cache[instrument].update(score)

    if SUPERVISED_MODEL_CACHE.get("key") == cache_key:
        apply_cached(SUPERVISED_MODEL_CACHE)
        return

    x_rows: list[list[float]] = []
    y_rows: list[list[float]] = []
    times: list[str] = []
    target_times: list[str] = []
    current: dict[str, dict[str, Any]] = {}
    rejections: dict[str, str] = {}
    for instrument, features in feature_cache.items():
        data = _supervised_candle_data(
            ((candle_sets.get(instrument) or {}).get("M5") or []),
            features.get("pip"),
        )
        if data is None:
            rejections[instrument] = "invalid_or_insufficient_completed_m5_history"
            features["supervised_withheld_reason"] = rejections[instrument]
            continue
        x_rows.extend(data["x"])
        y_rows.extend(data["y"])
        times.extend(data["times"])
        target_times.extend(data["target_times"])
        current[instrument] = data
    if len(x_rows) < 500 or not current:
        return

    shared_asof = max(item["model_decision_time"] for item in current.values())
    for instrument in list(current):
        if current[instrument]["model_decision_time"] != shared_asof:
            rejections[instrument] = "stale_m5_context_for_shared_fit"
            feature_cache[instrument]["supervised_withheld_reason"] = rejections[instrument]
            del current[instrument]
    assert all(target <= shared_asof for target in target_times)
    x = np.asarray(x_rows, dtype=float)
    y = np.asarray(y_rows, dtype=float)
    time_array = np.asarray(times, dtype="U40")
    unique_times = np.unique(time_array)
    if len(unique_times) < 20:
        return
    cutoff = unique_times[max(1, int(len(unique_times) * 0.80))]
    target_time_array = np.asarray(target_times, dtype="U40")
    train_mask = (time_array < cutoff) & (target_time_array < cutoff)
    validation_mask = time_array >= cutoff
    if int(np.sum(train_mask)) < 400 or int(np.sum(validation_mask)) < 100:
        return

    validation_model = _fit_ridge(x[train_mask], y[train_mask])
    validation_predictions = _predict_ridge(validation_model, x[validation_mask])
    validation_targets = y[validation_mask]
    chosen_side = np.argmax(validation_predictions, axis=1)
    chosen_prediction = validation_predictions[np.arange(len(validation_predictions)), chosen_side]
    chosen_target = validation_targets[np.arange(len(validation_targets)), chosen_side]
    top_count = min(len(chosen_prediction), max(20, int(len(chosen_prediction) * 0.10)))
    top_indices = np.argsort(chosen_prediction)[-top_count:]
    validation_hit_rate = float(np.mean(chosen_target[top_indices] > 0.0))
    validation_avg_norm = float(np.mean(chosen_target[top_indices]))

    final_model = _fit_ridge(x, y)
    instruments = sorted(current)
    current_x = np.asarray([current[instrument]["current_x"] for instrument in instruments], dtype=float)
    predictions = _predict_ridge(final_model, current_x)
    scores: dict[str, dict[str, Any]] = {}
    for index, instrument in enumerate(instruments):
        atr = safe_float(current[instrument]["current_atr_pips"])
        long_pips = float(predictions[index, 0] * atr)
        short_pips = float(predictions[index, 1] * atr)
        direction = "buy" if long_pips >= short_pips else "sell"
        scores[instrument] = {
            "supervised_ready": True,
            "supervised_withheld_reason": "",
            "supervised_direction": direction,
            "supervised_expected_net_pips": max(long_pips, short_pips),
            "supervised_long_net_pips": long_pips,
            "supervised_short_net_pips": short_pips,
            "supervised_model_candle_time": current[instrument]["model_candle_time"],
            "supervised_model_decision_time": current[instrument]["model_decision_time"],
            "supervised_data_quality": current[instrument]["data_quality"],
        }
    ranked = sorted(scores, key=lambda instrument: safe_float(scores[instrument]["supervised_expected_net_pips"]))
    denominator = max(1, len(ranked))
    for rank, instrument in enumerate(ranked, start=1):
        scores[instrument]["supervised_rank_percentile"] = rank / denominator
    meta = {
        "supervised_data_contract": SUPERVISED_DATA_CONTRACT,
        "supervised_model_asof_time": shared_asof,
        "supervised_model_rows": len(x),
        "supervised_purged_training_rows": int(np.sum((time_array < cutoff) & ~train_mask)),
        "supervised_validation_rows": int(np.sum(validation_mask)),
        "supervised_validation_hit_rate": validation_hit_rate,
        "supervised_validation_avg_norm": validation_avg_norm,
        "supervised_top_instrument": ranked[-1],
        "supervised_top_expected_net_pips": safe_float(scores[ranked[-1]]["supervised_expected_net_pips"]),
    }
    SUPERVISED_MODEL_CACHE.clear()
    SUPERVISED_MODEL_CACHE.update({"key": cache_key, "scores": scores, "meta": meta, "rejections": rejections})
    apply_cached(SUPERVISED_MODEL_CACHE)


def _pattern_forecast_key(mode: str, order: int, horizon_min: int) -> str:
    return f"{mode}:{order}:{horizon_min}"


def augment_pattern_count_features(
    feature_cache: dict[str, dict[str, Any]],
    candle_sets: dict[str, dict[str, list[dict[str, Any]]]],
    pip_sizes: dict[str, float],
    lanes: list[LaneSpec],
    forecaster: PatternCountForecaster | None,
) -> dict[str, int]:
    for features in feature_cache.values():
        features["pattern_count_forecasts"] = {}
    pattern_lanes = [lane for lane in lanes if lane.family == "pattern_count_forecast"]
    if forecaster is None or not pattern_lanes:
        return {"live_observations": 0, "forecasts": 0}

    live_observations = forecaster.observe_candle_sets(candle_sets, pip_sizes)
    if not forecaster.ready:
        return {"live_observations": live_observations, "forecasts": 0}
    requested = {
        (
            str(lane.parameters["pattern_mode"]),
            int(lane.parameters["pattern_order"]),
            int(lane.parameters["target_horizon_min"]),
        )
        for lane in pattern_lanes
    }
    forecast_count = 0
    for instrument, features in feature_cache.items():
        m1 = (candle_sets.get(instrument) or {}).get("M1") or []
        pip = safe_float(features.get("pip"), pip_sizes.get(instrument, 0.0001))
        forecasts: dict[str, dict[str, Any]] = {}
        for mode, order, horizon_min in requested:
            forecast = forecaster.forecast(
                instrument,
                m1,
                pip,
                mode=mode,
                order=order,
                horizon_min=horizon_min,
            )
            if forecast is not None:
                forecasts[_pattern_forecast_key(mode, order, horizon_min)] = forecast
                forecast_count += 1
        features["pattern_count_forecasts"] = forecasts
    return {"live_observations": live_observations, "forecasts": forecast_count}


def _crossed_above(left: list[float], right: list[float]) -> bool:
    return len(left) >= 2 and len(right) >= 2 and left[-2] <= right[-2] and left[-1] > right[-1]


def _crossed_below(left: list[float], right: list[float]) -> bool:
    return len(left) >= 2 and len(right) >= 2 and left[-2] >= right[-2] and left[-1] < right[-1]


def _recent_cross_age(left: list[float], right: list[float], *, above: bool, max_age: int) -> int | None:
    for age in range(max(0, max_age) + 1):
        current = len(left) - 1 - age
        previous = current - 1
        if previous < 0 or current >= len(right):
            continue
        crossed = (
            left[previous] <= right[previous] and left[current] > right[current]
            if above
            else left[previous] >= right[previous] and left[current] < right[current]
        )
        if crossed:
            return age
    return None


def _stochastic_series(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    period: int,
    smooth: int,
) -> tuple[list[float | None], list[float | None]]:
    raw: list[float | None] = []
    for index, close in enumerate(closes):
        if index + 1 < period:
            raw.append(None)
            continue
        window_high = max(highs[index - period + 1 : index + 1])
        window_low = min(lows[index - period + 1 : index + 1])
        raw.append(50.0 if math.isclose(window_high, window_low) else 100.0 * (close - window_low) / (window_high - window_low))

    def smoothed(values: list[float | None]) -> list[float | None]:
        output: list[float | None] = []
        for index in range(len(values)):
            window = values[max(0, index - smooth + 1) : index + 1]
            valid = [value for value in window if value is not None]
            output.append(statistics.fmean(valid) if len(valid) == smooth else None)
        return output

    k_values = smoothed(raw)
    return k_values, smoothed(k_values)


def _linear_regression_stats(values: list[float], pip: float) -> tuple[float, float]:
    if len(values) < 2:
        return 0.0, 0.0
    x_mean = (len(values) - 1) / 2.0
    y_mean = statistics.fmean(values)
    denominator = sum((index - x_mean) ** 2 for index in range(len(values)))
    if denominator <= 0.0:
        return 0.0, 0.0
    slope = sum((index - x_mean) * (value - y_mean) for index, value in enumerate(values)) / denominator
    fitted = [y_mean + slope * (index - x_mean) for index in range(len(values))]
    total = sum((value - y_mean) ** 2 for value in values)
    residual = sum((value - estimate) ** 2 for value, estimate in zip(values, fitted))
    r_squared = 0.0 if total <= 0.0 else max(0.0, min(1.0, 1.0 - residual / total))
    return slope / max(pip, 1e-12), r_squared


def _efficiency_ratio(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    path = sum(abs(values[index] - values[index - 1]) for index in range(1, len(values)))
    return 0.0 if path <= 0.0 else abs(values[-1] - values[0]) / path


def _pip_returns(values: list[float], pip: float) -> list[float]:
    scale = max(pip, 1e-12)
    return [
        (values[index] - values[index - 1]) / scale
        for index in range(1, len(values))
    ]


def _kalman_local_linear(
    values: list[float],
    pip: float,
    process_ratio: float,
    measurement_ratio: float,
) -> tuple[float, float, float, float] | None:
    if len(values) < 8:
        return None
    differences = [values[index] - values[index - 1] for index in range(1, len(values))]
    variance = max(pip * pip * 0.01, statistics.pvariance(differences))
    q_level = variance * max(1e-6, process_ratio)
    q_trend = q_level * 0.10
    measurement = variance * max(1e-6, measurement_ratio)
    level = values[0]
    trend = differences[0]
    p00 = measurement
    p01 = 0.0
    p10 = 0.0
    p11 = measurement
    innovation_z = 0.0
    for observed in values[1:]:
        predicted_level = level + trend
        predicted_trend = trend
        pp00 = p00 + p01 + p10 + p11 + q_level
        pp01 = p01 + p11
        pp10 = p10 + p11
        pp11 = p11 + q_trend
        innovation = observed - predicted_level
        innovation_variance = max(1e-18, pp00 + measurement)
        gain_level = pp00 / innovation_variance
        gain_trend = pp10 / innovation_variance
        level = predicted_level + gain_level * innovation
        trend = predicted_trend + gain_trend * innovation
        p00 = (1.0 - gain_level) * pp00
        p01 = (1.0 - gain_level) * pp01
        p10 = pp10 - gain_trend * pp00
        p11 = pp11 - gain_trend * pp01
        cross = 0.5 * (p01 + p10)
        p01 = cross
        p10 = cross
        innovation_z = innovation / math.sqrt(innovation_variance)
    forecast = level + trend
    return (
        (forecast - values[-1]) / max(pip, 1e-12),
        trend / max(pip, 1e-12),
        innovation_z,
        level,
    )


def _markov_sign_probability(
    values: list[float],
    order: int,
    laplace: float,
) -> tuple[float, int, str] | None:
    signs = [
        1 if values[index] > values[index - 1] else -1 if values[index] < values[index - 1] else 0
        for index in range(1, len(values))
    ]
    if len(signs) <= order or order <= 0:
        return None
    state = tuple(signs[-order:])
    up = 0
    down = 0
    for index in range(order, len(signs)):
        if tuple(signs[index - order : index]) != state:
            continue
        if signs[index] > 0:
            up += 1
        elif signs[index] < 0:
            down += 1
    total = up + down
    probability_up = (up + laplace) / max(1e-12, total + 2.0 * laplace)
    state_text = "".join("U" if value > 0 else "D" if value < 0 else "F" for value in state)
    return probability_up, total, state_text


def _online_ar_prediction(
    values: list[float],
    pip: float,
    lags: int,
    ridge: float,
) -> tuple[float, float, int] | None:
    returns = _pip_returns(values, pip)
    if len(returns) <= lags + 2 or lags <= 0:
        return None
    x_rows: list[list[float]] = []
    y_rows: list[float] = []
    for index in range(lags, len(returns)):
        x_rows.append(list(reversed(returns[index - lags : index])))
        y_rows.append(returns[index])
    x = np.asarray(x_rows, dtype=float)
    y = np.asarray(y_rows, dtype=float)
    mean = np.mean(x, axis=0)
    scale = np.std(x, axis=0)
    scale = np.where(scale < 1e-6, 1.0, scale)
    design = np.column_stack((np.ones(len(x)), (x - mean) / scale))
    penalty = np.eye(design.shape[1], dtype=float) * max(0.0, ridge)
    penalty[0, 0] = 0.0
    try:
        coefficients = np.linalg.solve(design.T @ design + penalty, design.T @ y)
    except np.linalg.LinAlgError:
        coefficients = np.linalg.pinv(design.T @ design + penalty) @ design.T @ y
    fitted = design @ coefficients
    total = float(np.sum((y - np.mean(y)) ** 2))
    residual = float(np.sum((y - fitted) ** 2))
    r_squared = 0.0 if total <= 1e-12 else 1.0 - residual / total
    current = np.asarray(list(reversed(returns[-lags:])), dtype=float)
    forecast = float(np.concatenate(([1.0], (current - mean) / scale)) @ coefficients)
    return forecast, r_squared, len(y)


def _variance_ratio(returns: list[float], aggregation: int) -> float | None:
    if aggregation <= 1 or len(returns) < aggregation * 4:
        return None
    centered = [value - statistics.fmean(returns) for value in returns]
    one_step = statistics.pvariance(centered)
    if one_step <= 1e-12:
        return None
    aggregated = [
        sum(centered[index - aggregation + 1 : index + 1])
        for index in range(aggregation - 1, len(centered))
    ]
    return statistics.pvariance(aggregated) / (aggregation * one_step)


def _permutation_entropy(values: list[float], embedding: int) -> tuple[float, float, int] | None:
    if embedding < 2 or len(values) < embedding * 3:
        return None
    counts: Counter[tuple[int, ...]] = Counter()
    for index in range(embedding - 1, len(values)):
        window = values[index - embedding + 1 : index + 1]
        order = tuple(sorted(range(embedding), key=lambda offset: (window[offset], offset)))
        counts[order] += 1
    total = sum(counts.values())
    if total <= 0:
        return None
    entropy = -sum((count / total) * math.log(count / total) for count in counts.values())
    maximum = math.log(math.factorial(embedding))
    normalized = 0.0 if maximum <= 0.0 else entropy / maximum
    return normalized, max(counts.values()) / total, total


def _cusum_scores(returns: list[float], drift_z: float) -> tuple[float, float, float] | None:
    if len(returns) < 12:
        return None
    mean = statistics.fmean(returns)
    deviation = statistics.pstdev(returns)
    if deviation <= 1e-9:
        return None
    positive = 0.0
    negative = 0.0
    last_z = 0.0
    for value in returns:
        last_z = (value - mean) / deviation
        positive = max(0.0, positive + last_z - drift_z)
        negative = min(0.0, negative + last_z + drift_z)
    return positive, abs(negative), last_z


def _har_volatility_forecast(
    returns: list[float],
    short_window: int,
    medium_window: int,
    long_window: int,
    ridge: float,
) -> tuple[float, float, int] | None:
    realized = [value * value for value in returns]
    if len(realized) <= long_window + 3:
        return None
    x_rows: list[list[float]] = []
    y_rows: list[float] = []
    for index in range(long_window, len(realized)):
        history = realized[:index]
        x_rows.append(
            [
                math.log1p(statistics.fmean(history[-short_window:])),
                math.log1p(statistics.fmean(history[-medium_window:])),
                math.log1p(statistics.fmean(history[-long_window:])),
            ]
        )
        y_rows.append(math.log1p(realized[index]))
    x = np.asarray(x_rows, dtype=float)
    y = np.asarray(y_rows, dtype=float)
    design = np.column_stack((np.ones(len(x)), x))
    penalty = np.eye(design.shape[1], dtype=float) * max(0.0, ridge)
    penalty[0, 0] = 0.0
    try:
        coefficients = np.linalg.solve(design.T @ design + penalty, design.T @ y)
    except np.linalg.LinAlgError:
        coefficients = np.linalg.pinv(design.T @ design + penalty) @ design.T @ y
    current = np.asarray(
        [
            1.0,
            math.log1p(statistics.fmean(realized[-short_window:])),
            math.log1p(statistics.fmean(realized[-medium_window:])),
            math.log1p(statistics.fmean(realized[-long_window:])),
        ],
        dtype=float,
    )
    forecast_variance = max(0.0, math.expm1(float(current @ coefficients)))
    baseline_variance = max(1e-12, statistics.fmean(realized[-long_window:]))
    return math.sqrt(forecast_variance), math.sqrt(forecast_variance / baseline_variance), len(y)


def _bipower_jump_stats(returns: list[float]) -> tuple[float, float, float, float, float] | None:
    if len(returns) < 12:
        return None
    history = returns[:-1]
    if len(history) < 10:
        return None
    realized_variance = sum(value * value for value in history)
    bipower_variance = (math.pi / 2.0) * sum(
        abs(history[index]) * abs(history[index - 1])
        for index in range(1, len(history))
    )
    continuous_sigma = math.sqrt(max(1e-12, bipower_variance / max(1, len(history) - 1)))
    jump_return = history[-1]
    reversal_return = returns[-1]
    jump_z = abs(jump_return) / continuous_sigma
    jump_share = max(0.0, realized_variance - bipower_variance) / max(1e-12, realized_variance)
    return jump_z, jump_share, jump_return, reversal_return, continuous_sigma


def _garch_volatility_forecast(
    returns: list[float],
    alpha: float,
    beta: float,
) -> tuple[float, float] | None:
    if len(returns) < 12 or alpha < 0.0 or beta < 0.0 or alpha + beta >= 1.0:
        return None
    unconditional = max(1e-12, statistics.fmean(value * value for value in returns))
    omega = (1.0 - alpha - beta) * unconditional
    variance = unconditional
    for value in returns:
        variance = omega + alpha * value * value + beta * variance
    forecast_volatility = math.sqrt(max(1e-12, variance))
    return forecast_volatility, forecast_volatility / math.sqrt(unconditional)


def _rescaled_range_hurst(returns: list[float]) -> tuple[float, int] | None:
    scales = [scale for scale in (8, 12, 16, 24, 32) if scale <= len(returns)]
    points: list[tuple[float, float]] = []
    for scale in scales:
        ratios: list[float] = []
        for start in range(0, len(returns) - scale + 1, scale):
            window = returns[start : start + scale]
            deviation = statistics.pstdev(window)
            if deviation <= 1e-9:
                continue
            mean = statistics.fmean(window)
            cumulative: list[float] = []
            running = 0.0
            for value in window:
                running += value - mean
                cumulative.append(running)
            span = max(cumulative) - min(cumulative)
            if span > 0.0:
                ratios.append(span / deviation)
        if ratios:
            points.append((math.log(scale), math.log(statistics.fmean(ratios))))
    if len(points) < 3:
        return None
    x = [point[0] for point in points]
    y = [point[1] for point in points]
    x_mean = statistics.fmean(x)
    y_mean = statistics.fmean(y)
    denominator = sum((value - x_mean) ** 2 for value in x)
    if denominator <= 0.0:
        return None
    hurst = sum((xv - x_mean) * (yv - y_mean) for xv, yv in points) / denominator
    return max(0.0, min(1.5, hurst)), len(points)


def _theil_sen_stats(values: list[float], pip: float) -> tuple[float, float, float] | None:
    if len(values) < 8:
        return None
    slopes = [
        (values[right] - values[left]) / (right - left)
        for left in range(len(values) - 1)
        for right in range(left + 1, len(values))
    ]
    slope = statistics.median(slopes)
    intercept = statistics.median([value - slope * index for index, value in enumerate(values)])
    residual_pips = statistics.median(
        abs(value - (intercept + slope * index)) / max(pip, 1e-12)
        for index, value in enumerate(values)
    )
    slope_pips = slope / max(pip, 1e-12)
    total_move = max(0.01, abs(slope_pips) * max(1, len(values) - 1))
    return slope_pips, residual_pips, residual_pips / total_move


def _tick_vwap(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    volumes: list[float],
) -> float | None:
    if not highs or not (len(highs) == len(lows) == len(closes) == len(volumes)):
        return None
    total_volume = sum(max(0.0, value) for value in volumes)
    if total_volume <= 0.0:
        return None
    return sum(
        ((high + low + close) / 3.0) * max(0.0, volume)
        for high, low, close, volume in zip(highs, lows, closes, volumes)
    ) / total_volume


def ahl_multihorizon_trend_setup(
    features: dict[str, Any],
    parameters: dict[str, Any],
) -> RawSetup:
    """Build a causal daily multi-horizon trend score and sizing audit."""

    closes = [safe_float(value) for value in (features.get("d1_closes") or [])]
    raw_lookbacks = parameters.get("lookbacks_days", "5,10,21,42")
    if isinstance(raw_lookbacks, str):
        lookbacks = tuple(
            int(value)
            for value in re.split(r"[\s,]+", raw_lookbacks.strip())
            if value
        )
    else:
        lookbacks = tuple(int(value) for value in raw_lookbacks)
    lookbacks = tuple(sorted({value for value in lookbacks if value > 0}))
    if not lookbacks or len(closes) <= max(lookbacks):
        return RawSetup(
            None,
            "insufficient_daily_trend_candles",
            {
                "research_shadow": True,
                "input_timeframe": "D1",
                "daily_rows": len(closes),
                "lookbacks_days": list(lookbacks),
            },
        )

    current = closes[-1]
    votes = {
        str(lookback): 1 if current > closes[-1 - lookback] else -1
        for lookback in lookbacks
        if not math.isclose(current, closes[-1 - lookback])
    }
    score = sum(votes.values())
    min_abs_score = max(1, int(safe_float(parameters.get("min_abs_score"), 2)))
    if abs(score) < min_abs_score:
        return RawSetup(
            None,
            "daily_trend_score_neutral",
            {
                "research_shadow": True,
                "input_timeframe": "D1",
                "trend_score": score,
                "lookback_votes": votes,
                "lookbacks_days": list(lookbacks),
            },
        )

    volatility_lookback = max(
        5,
        int(safe_float(parameters.get("volatility_lookback_days"), 20)),
    )
    recent = closes[-(volatility_lookback + 1) :]
    log_returns = [
        math.log(current_close / previous_close)
        for previous_close, current_close in zip(recent, recent[1:])
        if previous_close > 0.0 and current_close > 0.0
    ]
    daily_volatility = (
        statistics.pstdev(log_returns) if len(log_returns) >= 2 else 0.0
    )
    annualized_volatility = daily_volatility * math.sqrt(252.0)
    target_volatility = max(
        0.0,
        safe_float(parameters.get("target_annualized_volatility"), 0.10),
    )
    volatility_scalar = (
        min(2.0, target_volatility / annualized_volatility)
        if annualized_volatility > 1e-12
        else 0.0
    )
    conviction_fraction = min(1.0, abs(score) / max(1, len(lookbacks)))
    research_risk_weight = conviction_fraction * volatility_scalar
    pip = max(1e-12, safe_float(features.get("pip"), 0.0001))
    daily_candles = features.get("d1_candles") or []
    daily_atr_pips = local_atr_pips(
        daily_candles,
        pip,
        min(volatility_lookback, max(2, len(daily_candles) - 1)),
    )
    return RawSetup(
        "buy" if score > 0 else "sell",
        "",
        {
            "research_shadow": True,
            "source_method": "public_man_ahl_multihorizon_trend_example",
            "input_timeframe": "D1",
            "decision_candle_time": str(features.get("d1_origin") or ""),
            "reference_horizon_sec": 86400,
            "lookbacks_days": list(lookbacks),
            "lookback_votes": votes,
            "trend_score": score,
            "conviction_fraction": round(conviction_fraction, 6),
            "annualized_realized_volatility": round(annualized_volatility, 8),
            "target_annualized_volatility": round(target_volatility, 8),
            "volatility_scalar_capped": round(volatility_scalar, 6),
            "research_risk_weight": round(research_risk_weight, 6),
            "daily_atr_pips": round(daily_atr_pips, 4),
            "strength_pips": round(conviction_fraction * daily_atr_pips, 4),
        },
    )


def evaluate_family(lane: LaneSpec, features: dict[str, Any]) -> RawSetup:
    p = lane.parameters
    closes = features["closes"]
    m5_closes = features["m5_closes"]
    opens = features.get("opens") or closes
    highs = features["highs"]
    lows = features["lows"]
    volumes = features.get("volumes") or [0.0] * len(closes)
    pip = safe_float(features["pip"], 0.0001)
    r1 = safe_float(features["r1_pips"])
    r3 = safe_float(features["r3_pips"])
    r5 = safe_float(features["r5_pips"])
    m5_r3 = safe_float(features["m5_r3_pips"])
    pos20 = safe_float(features["pos20"], 0.5)
    details: dict[str, Any] = {
        "r1_pips": round(r1, 3),
        "r3_pips": round(r3, 3),
        "r5_pips": round(r5, 3),
        "m5_r3_pips": round(m5_r3, 3),
        "pos20": round(pos20, 4),
        "m1_atr14_pips": round(safe_float(features["m1_atr14_pips"]), 3),
        "m5_atr14_pips": round(safe_float(features["m5_atr14_pips"]), 3),
        "volume_ratio_12": round(safe_float(features.get("volume_ratio_12"), 1.0), 4),
        "volume_ratio_30": round(safe_float(features.get("volume_ratio_30"), 1.0), 4),
        "current_candle_spread_pips": round(safe_float(features.get("current_candle_spread_pips")), 3),
        "spread_ratio_12": round(safe_float(features.get("spread_ratio_12"), 1.0), 4),
        "spread_drop_3_pips": round(safe_float(features.get("spread_drop_3_pips")), 3),
        "signal_candle_time": features["candle_time"],
    }
    if lane.family in RESEARCH_SHADOW_FAMILIES:
        details["research_shadow"] = True
    direction: str | None = None

    if lane.family == "ahl_multihorizon_trend":
        return ahl_multihorizon_trend_setup(features, p)

    if lane.family == "pattern_count_forecast":
        mode = str(p["pattern_mode"])
        order = int(p["pattern_order"])
        horizon_min = int(p["target_horizon_min"])
        key = _pattern_forecast_key(mode, order, horizon_min)
        forecast = (features.get("pattern_count_forecasts") or {}).get(key)
        if not isinstance(forecast, dict):
            return RawSetup(None, "pattern_model_unavailable", details)
        details.update(
            {
                "pattern_forecast": forecast,
                "decision_candle_time": str(forecast.get("decision_candle_time") or features["candle_time"]),
                "strength_pips": safe_float(forecast.get("expected_abs_move_pips")),
                "pattern": str(forecast.get("pattern") or ""),
                "pattern_probability_up": safe_float(forecast.get("probability_up"), 0.5),
                "pattern_movement_coefficient": safe_float(forecast.get("movement_coefficient")),
            }
        )
        forecast_direction = str(forecast.get("predicted_direction") or "")
        if forecast_direction not in {"buy", "sell"}:
            return RawSetup(None, "pattern_direction_unavailable", details)
        direction = forecast_direction

    elif lane.family == "momentum":
        if r1 >= p["min_r1"] and r3 >= p["min_r3"] and r5 >= p["min_r5"] and m5_r3 >= p["min_m5_r3"] and pos20 >= p["pos_long"]:
            direction = "buy"
        elif r1 <= -p["min_r1"] and r3 <= -p["min_r3"] and r5 <= -p["min_r5"] and m5_r3 <= -p["min_m5_r3"] and pos20 <= p["pos_short"]:
            direction = "sell"

    elif lane.family == "pullback":
        m1_ema = ema_series(closes, int(p["m1_ema"]))
        m5_fast = ema_series(m5_closes, int(p["m5_fast"]))
        m5_slow = ema_series(m5_closes, int(p["m5_slow"]))
        details.update({"m1_ema": round(m1_ema[-1], 6), "m5_fast": round(m5_fast[-1], 6), "m5_slow": round(m5_slow[-1], 6)})
        if m5_fast[-1] > m5_slow[-1] and m5_r3 >= p["min_m5_r3"] and closes[-2] <= m1_ema[-2] and closes[-1] > m1_ema[-1] and r1 >= p["min_r1"] and p["pos_low"] <= pos20 <= p["pos_high"]:
            direction = "buy"
        elif m5_fast[-1] < m5_slow[-1] and m5_r3 <= -p["min_m5_r3"] and closes[-2] >= m1_ema[-2] and closes[-1] < m1_ema[-1] and r1 <= -p["min_r1"] and 1.0 - p["pos_high"] <= pos20 <= 1.0 - p["pos_low"]:
            direction = "sell"

    elif lane.family == "macd_rsi_reversal":
        m10 = features["m10_closes"]
        m30 = features["m30_closes"]
        if len(m10) < 20 or len(m30) < 26:
            return RawSetup(None, "insufficient_multitimeframe_candles", details)
        m1_rsi = rsi_series(closes)
        m10_rsi = rsi_series(m10)
        hist = macd_histogram(closes)
        m30_fast = ema_series(m30, 12)
        m30_slow = ema_series(m30, 26)
        previous_rsi = m1_rsi[-2]
        current_rsi = m1_rsi[-1]
        context_rsi = m10_rsi[-1]
        details.update({
            "m1_rsi14": None if current_rsi is None else round(current_rsi, 2),
            "m10_rsi14": None if context_rsi is None else round(context_rsi, 2),
            "macd_hist": round(hist[-1], 8),
            "macd_hist_prev": round(hist[-2], 8),
            "m30_ema12": round(m30_fast[-1], 6),
            "m30_ema26": round(m30_slow[-1], 6),
        })
        if previous_rsi is not None and current_rsi is not None and context_rsi is not None:
            if m30_fast[-1] >= m30_slow[-1] and context_rsi <= p["context_low"] and previous_rsi <= p["rsi_low"] < current_rsi and hist[-1] > hist[-2]:
                direction = "buy"
            elif m30_fast[-1] <= m30_slow[-1] and context_rsi >= p["context_high"] and previous_rsi >= p["rsi_high"] > current_rsi and hist[-1] < hist[-2]:
                direction = "sell"

    elif lane.family == "ema_trend_cross":
        m1_fast = ema_series(closes, int(p["m1_fast"]))
        m1_slow = ema_series(closes, int(p["m1_slow"]))
        m5_fast = ema_series(m5_closes, int(p["m5_fast"]))
        m5_slow = ema_series(m5_closes, int(p["m5_slow"]))
        details.update({"m1_fast": round(m1_fast[-1], 6), "m1_slow": round(m1_slow[-1], 6), "m5_fast": round(m5_fast[-1], 6), "m5_slow": round(m5_slow[-1], 6)})
        if _crossed_above(m1_fast, m1_slow) and m5_fast[-1] > m5_slow[-1] and m5_r3 >= p["min_m5_r3"]:
            direction = "buy"
        elif _crossed_below(m1_fast, m1_slow) and m5_fast[-1] < m5_slow[-1] and m5_r3 <= -p["min_m5_r3"]:
            direction = "sell"

    elif lane.family == "bollinger_reversion":
        period = int(p["period"])
        if len(closes) < period + 2:
            return RawSetup(None, "insufficient_bollinger_candles", details)
        previous_window = closes[-period - 1 : -1]
        current_window = closes[-period:]
        previous_mean = statistics.fmean(previous_window)
        current_mean = statistics.fmean(current_window)
        previous_std = statistics.pstdev(previous_window)
        current_std = statistics.pstdev(current_window)
        previous_lower = previous_mean - safe_float(p["z"]) * previous_std
        previous_upper = previous_mean + safe_float(p["z"]) * previous_std
        current_lower = current_mean - safe_float(p["z"]) * current_std
        current_upper = current_mean + safe_float(p["z"]) * current_std
        rsi = rsi_series(closes)
        previous_rsi = rsi[-2]
        details.update({"bb_lower": round(current_lower, 6), "bb_upper": round(current_upper, 6), "previous_rsi14": None if previous_rsi is None else round(previous_rsi, 2)})
        if previous_rsi is not None and closes[-2] < previous_lower and closes[-1] >= current_lower and previous_rsi <= p["rsi_low"]:
            direction = "buy"
        elif previous_rsi is not None and closes[-2] > previous_upper and closes[-1] <= current_upper and previous_rsi >= p["rsi_high"]:
            direction = "sell"

    elif lane.family == "currency_strength":
        sample_count = int(safe_float(features.get("cross_sample_count")))
        strength_r3 = safe_float(features.get("cross_strength_r3"))
        strength_r5 = safe_float(features.get("cross_strength_r5"))
        breadth_r3 = safe_float(features.get("cross_breadth_r3"))
        pair_r1 = safe_float(features.get("pair_norm_r1"))
        pair_r3 = safe_float(features.get("pair_norm_r3"))
        details.update(
            {
                "cross_sample_count": sample_count,
                "cross_strength_r3": round(strength_r3, 4),
                "cross_strength_r5": round(strength_r5, 4),
                "cross_breadth_r3": round(breadth_r3, 4),
                "pair_norm_r1": round(pair_r1, 4),
                "pair_norm_r3": round(pair_r3, 4),
            }
        )
        if sample_count < int(p["min_cross_samples"]):
            return RawSetup(None, "insufficient_cross_section", details)
        if (
            strength_r3 >= p["min_strength_r3"]
            and strength_r5 >= p["min_strength_r5"]
            and breadth_r3 >= p["min_breadth"]
            and pair_r1 >= p["min_pair_r1"]
            and pair_r3 >= p["min_pair_r3"]
        ):
            direction = "buy"
        elif (
            strength_r3 <= -p["min_strength_r3"]
            and strength_r5 <= -p["min_strength_r5"]
            and breadth_r3 <= -p["min_breadth"]
            and pair_r1 <= -p["min_pair_r1"]
            and pair_r3 <= -p["min_pair_r3"]
        ):
            direction = "sell"

    elif lane.family == "relative_value_reversion":
        sample_count = int(safe_float(features.get("cross_sample_count")))
        strength_r3 = safe_float(features.get("cross_strength_r3"))
        residual_r3 = safe_float(features.get("relative_residual_r3"))
        residual_r5 = safe_float(features.get("relative_residual_r5"))
        pair_r1 = safe_float(features.get("pair_norm_r1"))
        details.update(
            {
                "cross_sample_count": sample_count,
                "cross_strength_r3": round(strength_r3, 4),
                "relative_residual_r3": round(residual_r3, 4),
                "relative_residual_r5": round(residual_r5, 4),
                "pair_norm_r1": round(pair_r1, 4),
            }
        )
        if sample_count < int(p["min_cross_samples"]):
            return RawSetup(None, "insufficient_cross_section", details)
        if abs(strength_r3) <= p["max_system_strength"]:
            if (
                residual_r3 <= -p["min_residual_r3"]
                and residual_r5 <= -p["min_residual_r5"]
                and pair_r1 >= p["min_reversal_r1"]
                and pos20 <= p["long_pos_max"]
            ):
                direction = "buy"
            elif (
                residual_r3 >= p["min_residual_r3"]
                and residual_r5 >= p["min_residual_r5"]
                and pair_r1 <= -p["min_reversal_r1"]
                and pos20 >= p["short_pos_min"]
            ):
                direction = "sell"

    elif lane.family == "supervised_return_rank":
        if not bool(features.get("supervised_ready")):
            return RawSetup(None, "supervised_model_unavailable", details)
        expected_net = safe_float(features.get("supervised_expected_net_pips"))
        long_net = safe_float(features.get("supervised_long_net_pips"))
        short_net = safe_float(features.get("supervised_short_net_pips"))
        rank_percentile = safe_float(features.get("supervised_rank_percentile"))
        model_rows = int(safe_float(features.get("supervised_model_rows")))
        validation_rows = int(safe_float(features.get("supervised_validation_rows")))
        validation_hit_rate = safe_float(features.get("supervised_validation_hit_rate"))
        validation_avg_norm = safe_float(features.get("supervised_validation_avg_norm"))
        model_direction = str(features.get("supervised_direction") or "")
        model_candle_time = str(features.get("supervised_model_candle_time") or "")
        details.update(
            {
                "supervised_expected_net_pips": round(expected_net, 4),
                "supervised_long_net_pips": round(long_net, 4),
                "supervised_short_net_pips": round(short_net, 4),
                "supervised_rank_percentile": round(rank_percentile, 4),
                "supervised_model_rows": model_rows,
                "supervised_validation_rows": validation_rows,
                "supervised_validation_hit_rate": round(validation_hit_rate, 4),
                "supervised_validation_avg_norm": round(validation_avg_norm, 4),
                "decision_candle_time": model_candle_time,
                "strength_pips": max(expected_net, abs(r1), abs(r3), abs(r5)),
            }
        )
        if model_rows < int(p["min_model_rows"]):
            return RawSetup(None, "insufficient_supervised_rows", details)
        if (
            validation_hit_rate < p["min_validation_hit_rate"]
            or validation_avg_norm < p["min_validation_avg_norm"]
        ):
            return RawSetup(None, "supervised_validation_gate", details)
        if (
            model_direction in {"buy", "sell"}
            and rank_percentile >= p["min_rank_percentile"]
            and expected_net >= p["min_expected_net_pips"]
        ):
            direction = model_direction

    elif lane.family == "higher_timeframe_alignment":
        m15 = features.get("m15_closes") or []
        h1 = features.get("h1_closes") or []
        if len(m15) < int(p["m15_slow"]) + 2 or len(h1) < int(p["h1_slow"]) + 2:
            return RawSetup(None, "insufficient_higher_timeframe_candles", details)
        m1_fast = ema_series(closes, int(p["m1_fast"]))
        m1_slow = ema_series(closes, int(p["m1_slow"]))
        m15_fast = ema_series(m15, int(p["m15_fast"]))
        m15_slow = ema_series(m15, int(p["m15_slow"]))
        h1_fast = ema_series(h1, int(p["h1_fast"]))
        h1_slow = ema_series(h1, int(p["h1_slow"]))
        max_cross_age = int(p["max_cross_age"])
        buy_cross_age = _recent_cross_age(m1_fast, m1_slow, above=True, max_age=max_cross_age)
        sell_cross_age = _recent_cross_age(m1_fast, m1_slow, above=False, max_age=max_cross_age)
        atr = max(0.1, safe_float(features.get("m5_atr14_pips")))
        m15_gap = (m15_fast[-1] - m15_slow[-1]) / pip / atr
        h1_gap = (h1_fast[-1] - h1_slow[-1]) / pip / atr
        m15_slope = (m15_fast[-1] - m15_fast[-2]) / pip / atr
        h1_slope = (h1_fast[-1] - h1_fast[-2]) / pip / atr
        details.update(
            {
                "m1_buy_cross_age": buy_cross_age,
                "m1_sell_cross_age": sell_cross_age,
                "m15_gap_atr": round(m15_gap, 4),
                "h1_gap_atr": round(h1_gap, 4),
                "m15_slope_atr": round(m15_slope, 4),
                "h1_slope_atr": round(h1_slope, 4),
            }
        )
        if (
            buy_cross_age is not None
            and r1 >= p["min_r1"]
            and m15_gap >= p["min_m15_gap_atr"]
            and h1_gap >= p["min_h1_gap_atr"]
            and m15_slope >= 0.0
            and h1_slope >= 0.0
        ):
            direction = "buy"
        elif (
            sell_cross_age is not None
            and r1 <= -p["min_r1"]
            and m15_gap <= -p["min_m15_gap_atr"]
            and h1_gap <= -p["min_h1_gap_atr"]
            and m15_slope <= 0.0
            and h1_slope <= 0.0
        ):
            direction = "sell"

    elif lane.family == "donchian_breakout":
        period = int(p["period"])
        if len(highs) < period + 1:
            return RawSetup(None, "insufficient_donchian_candles", details)
        upper = max(highs[-period - 1 : -1])
        lower = min(lows[-period - 1 : -1])
        buffer = safe_float(p["buffer_pips"]) * pip
        details.update({"donchian_upper": round(upper, 6), "donchian_lower": round(lower, 6)})
        if closes[-1] > upper + buffer and m5_r3 >= p["min_m5_r3"]:
            direction = "buy"
        elif closes[-1] < lower - buffer and m5_r3 <= -p["min_m5_r3"]:
            direction = "sell"

    elif lane.family == "rsi_trend_continuation":
        fast = ema_series(closes, int(p["ema_fast"]))
        slow = ema_series(closes, int(p["ema_slow"]))
        rsi = rsi_series(closes)
        previous_rsi = rsi[-2]
        current_rsi = rsi[-1]
        details.update(
            {
                "trend_fast": round(fast[-1], 6),
                "trend_slow": round(slow[-1], 6),
                "previous_rsi14": None if previous_rsi is None else round(previous_rsi, 2),
                "rsi14": None if current_rsi is None else round(current_rsi, 2),
            }
        )
        if previous_rsi is not None and current_rsi is not None:
            sell_trigger = 100.0 - p["rsi_trigger"]
            if (
                fast[-1] > slow[-1]
                and previous_rsi <= p["rsi_trigger"] < current_rsi <= p["max_rsi"]
                and r1 >= p["min_r1"]
                and m5_r3 >= p["min_m5_r3"]
            ):
                direction = "buy"
            elif (
                fast[-1] < slow[-1]
                and previous_rsi >= sell_trigger > current_rsi >= 100.0 - p["max_rsi"]
                and r1 <= -p["min_r1"]
                and m5_r3 <= -p["min_m5_r3"]
            ):
                direction = "sell"

    elif lane.family == "stochastic_reversal":
        k_values, d_values = _stochastic_series(
            highs,
            lows,
            closes,
            int(p["period"]),
            int(p["smooth"]),
        )
        previous_k, current_k = k_values[-2:]
        previous_d, current_d = d_values[-2:]
        details.update(
            {
                "stochastic_k": None if current_k is None else round(current_k, 2),
                "stochastic_d": None if current_d is None else round(current_d, 2),
                "stochastic_k_prev": None if previous_k is None else round(previous_k, 2),
                "stochastic_d_prev": None if previous_d is None else round(previous_d, 2),
            }
        )
        if None not in (previous_k, current_k, previous_d, current_d):
            if (
                previous_k <= previous_d
                and current_k > current_d
                and min(previous_k, previous_d) <= p["oversold"]
                and r1 >= p["min_r1"]
                and pos20 <= p["long_pos_max"]
            ):
                direction = "buy"
            elif (
                previous_k >= previous_d
                and current_k < current_d
                and max(previous_k, previous_d) >= p["overbought"]
                and r1 <= -p["min_r1"]
                and pos20 >= p["short_pos_min"]
            ):
                direction = "sell"

    elif lane.family == "volatility_squeeze_breakout":
        squeeze_period = int(p["squeeze_period"])
        baseline_period = int(p["baseline_period"])
        breakout_period = int(p["breakout_period"])
        required = max(squeeze_period, baseline_period, breakout_period) + 1
        if len(closes) < required:
            return RawSetup(None, "insufficient_squeeze_candles", details)
        squeeze_std = statistics.pstdev(closes[-squeeze_period - 1 : -1])
        baseline_std = statistics.pstdev(closes[-baseline_period - 1 : -1])
        if baseline_std <= 0.0:
            return RawSetup(None, "flat_volatility_baseline", details)
        width_ratio = squeeze_std / baseline_std
        upper = max(highs[-breakout_period - 1 : -1])
        lower = min(lows[-breakout_period - 1 : -1])
        details.update(
            {
                "squeeze_width_ratio": round(width_ratio, 4),
                "squeeze_breakout_upper": round(upper, 6),
                "squeeze_breakout_lower": round(lower, 6),
            }
        )
        if (
            width_ratio <= p["max_width_ratio"]
            and closes[-1] > upper
            and r1 >= p["min_r1"]
            and m5_r3 >= p["min_m5_r3"]
        ):
            direction = "buy"
        elif (
            width_ratio <= p["max_width_ratio"]
            and closes[-1] < lower
            and r1 <= -p["min_r1"]
            and m5_r3 <= -p["min_m5_r3"]
        ):
            direction = "sell"

    elif lane.family == "range_expansion":
        lookback = int(p["lookback"])
        if len(closes) < lookback + 1:
            return RawSetup(None, "insufficient_range_candles", details)
        candle_range = max(0.0, highs[-1] - lows[-1])
        body = closes[-1] - opens[-1]
        body_fraction = abs(body) / max(candle_range, 1e-12)
        close_location = (closes[-1] - lows[-1]) / max(candle_range, 1e-12)
        atr_price = max(0.1, safe_float(features["m1_atr14_pips"])) * pip
        range_atr = candle_range / atr_price
        prior_ranges = [(highs[index] - lows[index]) / pip for index in range(-lookback - 1, -1)]
        details.update(
            {
                "range_atr": round(range_atr, 4),
                "range_pips": round(candle_range / pip, 3),
                "median_prior_range_pips": round(statistics.median(prior_ranges), 3),
                "body_fraction": round(body_fraction, 4),
                "close_location": round(close_location, 4),
                "strength_pips": round(abs(body) / pip, 3),
            }
        )
        if (
            body > 0.0
            and range_atr >= p["min_range_atr"]
            and body_fraction >= p["min_body_fraction"]
            and close_location >= p["close_extreme"]
            and r1 >= p["min_r1"]
        ):
            direction = "buy"
        elif (
            body < 0.0
            and range_atr >= p["min_range_atr"]
            and body_fraction >= p["min_body_fraction"]
            and close_location <= 1.0 - p["close_extreme"]
            and r1 <= -p["min_r1"]
        ):
            direction = "sell"

    elif lane.family == "candlestick_reversal":
        previous_open, current_open = opens[-2], opens[-1]
        previous_close, current_close = closes[-2], closes[-1]
        candle_range = max(0.0, highs[-1] - lows[-1])
        body = abs(current_close - current_open)
        body_pips = body / pip
        lower_wick = min(current_open, current_close) - lows[-1]
        upper_wick = highs[-1] - max(current_open, current_close)
        close_location = (current_close - lows[-1]) / max(candle_range, 1e-12)
        bullish_engulfing = (
            previous_close < previous_open
            and current_close > current_open
            and current_open <= previous_close
            and current_close >= previous_open
        )
        bearish_engulfing = (
            previous_close > previous_open
            and current_close < current_open
            and current_open >= previous_close
            and current_close <= previous_open
        )
        bullish_pin = lower_wick >= p["min_wick_body"] * max(body, 1e-12) and close_location >= 0.65
        bearish_pin = upper_wick >= p["min_wick_body"] * max(body, 1e-12) and close_location <= 0.35
        details.update(
            {
                "bullish_engulfing": bullish_engulfing,
                "bearish_engulfing": bearish_engulfing,
                "lower_wick_body": round(lower_wick / max(body, 1e-12), 3),
                "upper_wick_body": round(upper_wick / max(body, 1e-12), 3),
                "candle_body_pips": round(body_pips, 3),
                "strength_pips": round(max(body_pips, abs(r1)), 3),
            }
        )
        if (
            (bullish_engulfing or bullish_pin)
            and body_pips >= p["min_body_pips"]
            and pos20 <= p["long_pos_max"]
            and r1 >= p["min_r1"]
        ):
            direction = "buy"
        elif (
            (bearish_engulfing or bearish_pin)
            and body_pips >= p["min_body_pips"]
            and pos20 >= p["short_pos_min"]
            and r1 <= -p["min_r1"]
        ):
            direction = "sell"

    elif lane.family == "linear_regression_trend":
        period = int(p["period"])
        if len(closes) < period:
            return RawSetup(None, "insufficient_regression_candles", details)
        slope_pips, r_squared = _linear_regression_stats(closes[-period:], pip)
        details.update(
            {
                "regression_slope_pips": round(slope_pips, 4),
                "regression_r2": round(r_squared, 4),
                "strength_pips": round(max(abs(slope_pips) * 3.0, abs(r3)), 3),
            }
        )
        if slope_pips >= p["min_slope_pips"] and r_squared >= p["min_r2"] and r1 >= p["min_r1"]:
            direction = "buy"
        elif slope_pips <= -p["min_slope_pips"] and r_squared >= p["min_r2"] and r1 <= -p["min_r1"]:
            direction = "sell"

    elif lane.family == "volume_impulse":
        lookback = int(p["lookback"])
        if len(volumes) < lookback + 1:
            return RawSetup(None, "insufficient_volume_candles", details)
        baseline_volume = statistics.median(volumes[-lookback - 1 : -1])
        if baseline_volume <= 0.0:
            return RawSetup(None, "volume_unavailable", details)
        volume_ratio = safe_float(volumes[-1]) / baseline_volume
        body = closes[-1] - opens[-1]
        body_atr = abs(body) / (max(0.1, safe_float(features["m1_atr14_pips"])) * pip)
        candle_range = max(0.0, highs[-1] - lows[-1])
        close_location = (closes[-1] - lows[-1]) / max(candle_range, 1e-12)
        details.update(
            {
                "volume_ratio": round(volume_ratio, 4),
                "body_atr": round(body_atr, 4),
                "close_location": round(close_location, 4),
                "strength_pips": round(abs(body) / pip, 3),
            }
        )
        if (
            body > 0.0
            and volume_ratio >= p["min_volume_ratio"]
            and body_atr >= p["min_body_atr"]
            and close_location >= p["close_extreme"]
        ):
            direction = "buy"
        elif (
            body < 0.0
            and volume_ratio >= p["min_volume_ratio"]
            and body_atr >= p["min_body_atr"]
            and close_location <= 1.0 - p["close_extreme"]
        ):
            direction = "sell"

    elif lane.family == "atr_mean_reversion":
        period = int(p["period"])
        mean = ema_series(closes, period)
        atr = max(0.1, safe_float(features["m1_atr14_pips"]))
        previous_z = (closes[-2] - mean[-2]) / pip / atr
        current_z = (closes[-1] - mean[-1]) / pip / atr
        m5_trend_atr = m5_r3 / max(0.1, safe_float(features["m5_atr14_pips"]))
        details.update(
            {
                "previous_displacement_atr": round(previous_z, 4),
                "current_displacement_atr": round(current_z, 4),
                "m5_trend_atr": round(m5_trend_atr, 4),
                "strength_pips": round(max(abs(previous_z) * atr, abs(r1)), 3),
            }
        )
        if (
            previous_z <= -p["entry_atr"]
            and r1 >= p["min_reversal_pips"]
            and abs(m5_trend_atr) <= p["max_m5_trend_atr"]
        ):
            direction = "buy"
        elif (
            previous_z >= p["entry_atr"]
            and r1 <= -p["min_reversal_pips"]
            and abs(m5_trend_atr) <= p["max_m5_trend_atr"]
        ):
            direction = "sell"

    elif lane.family == "cross_sectional_pair_rank":
        pair_count = int(safe_float(features.get("cross_pair_count")))
        rank_r3 = safe_float(features.get("pair_rank_r3"), 0.5)
        rank_r5 = safe_float(features.get("pair_rank_r5"), 0.5)
        details.update(
            {
                "cross_pair_count": pair_count,
                "pair_rank_r3": round(rank_r3, 4),
                "pair_rank_r5": round(rank_r5, 4),
            }
        )
        if pair_count < int(p["min_pairs"]):
            return RawSetup(None, "insufficient_pair_rank_sample", details)
        if rank_r3 >= p["min_rank"] and rank_r5 >= p["min_rank"] and r1 >= p["min_r1"]:
            direction = "buy"
        elif rank_r3 <= 1.0 - p["min_rank"] and rank_r5 <= 1.0 - p["min_rank"] and r1 <= -p["min_r1"]:
            direction = "sell"

    elif lane.family == "regime_switching":
        period = int(p["period"])
        if len(closes) < period:
            return RawSetup(None, "insufficient_regime_candles", details)
        window = closes[-period:]
        efficiency = _efficiency_ratio(window)
        mean = statistics.fmean(window)
        std = statistics.pstdev(window)
        displacement_z = 0.0 if std <= 0.0 else (closes[-1] - mean) / std
        regime = "neutral"
        if efficiency >= p["trend_er"]:
            regime = "trend"
            if r3 >= p["trend_min_r3"] and r5 > 0.0 and r1 >= p["min_r1"]:
                direction = "buy"
            elif r3 <= -p["trend_min_r3"] and r5 < 0.0 and r1 <= -p["min_r1"]:
                direction = "sell"
        elif efficiency <= p["reversion_er"]:
            regime = "mean_reversion"
            if displacement_z <= -p["reversion_z"] and r1 >= p["min_r1"]:
                direction = "buy"
            elif displacement_z >= p["reversion_z"] and r1 <= -p["min_r1"]:
                direction = "sell"
        details.update(
            {
                "regime": regime,
                "efficiency_ratio": round(efficiency, 4),
                "regime_displacement_z": round(displacement_z, 4),
            }
        )

    elif lane.family == "failed_breakout_reversal":
        lookback = int(p["lookback"])
        if len(closes) < lookback + 1:
            return RawSetup(None, "insufficient_failed_breakout_candles", details)
        prior_high = max(highs[-lookback - 1 : -1])
        prior_low = min(lows[-lookback - 1 : -1])
        buffer = safe_float(p["breakout_buffer_pips"]) * pip
        candle_range = max(0.0, highs[-1] - lows[-1])
        body = closes[-1] - opens[-1]
        body_fraction = abs(body) / max(candle_range, 1e-12)
        upper_tail = highs[-1] - max(opens[-1], closes[-1])
        lower_tail = min(opens[-1], closes[-1]) - lows[-1]
        atr_price = max(0.1, safe_float(features["m1_atr14_pips"])) * pip
        upper_tail_atr = upper_tail / atr_price
        lower_tail_atr = lower_tail / atr_price
        details.update(
            {
                "failed_breakout_prior_high": round(prior_high, 6),
                "failed_breakout_prior_low": round(prior_low, 6),
                "body_fraction": round(body_fraction, 4),
                "upper_tail_atr": round(upper_tail_atr, 4),
                "lower_tail_atr": round(lower_tail_atr, 4),
                "strength_pips": round(max(abs(body), upper_tail, lower_tail) / pip, 3),
            }
        )
        if (
            lows[-1] < prior_low - buffer
            and closes[-1] > prior_low
            and body > 0.0
            and body_fraction >= p["min_rejection_body_fraction"]
            and lower_tail_atr >= p["min_tail_atr"]
            and pos20 <= p["long_pos_max"]
        ):
            direction = "buy"
        elif (
            highs[-1] > prior_high + buffer
            and closes[-1] < prior_high
            and body < 0.0
            and body_fraction >= p["min_rejection_body_fraction"]
            and upper_tail_atr >= p["min_tail_atr"]
            and pos20 >= p["short_pos_min"]
        ):
            direction = "sell"

    elif lane.family == "efficiency_filtered_momentum":
        period = int(p["period"])
        if len(closes) < period:
            return RawSetup(None, "insufficient_efficiency_candles", details)
        window = closes[-period:]
        efficiency = _efficiency_ratio(window)
        atr = max(0.1, safe_float(features["m1_atr14_pips"]))
        displacement_atr = abs(closes[-1] - statistics.fmean(window)) / pip / atr
        details.update(
            {
                "efficiency_ratio": round(efficiency, 4),
                "displacement_atr": round(displacement_atr, 4),
                "strength_pips": round(max(abs(r3), abs(r5)), 3),
            }
        )
        if (
            efficiency >= p["min_efficiency"]
            and displacement_atr <= p["max_displacement_atr"]
            and r3 >= p["min_r3"]
            and r5 >= p["min_r5"]
            and m5_r3 >= p["min_m5_r3"]
        ):
            direction = "buy"
        elif (
            efficiency >= p["min_efficiency"]
            and displacement_atr <= p["max_displacement_atr"]
            and r3 <= -p["min_r3"]
            and r5 <= -p["min_r5"]
            and m5_r3 <= -p["min_m5_r3"]
        ):
            direction = "sell"

    elif lane.family == "cross_pair_lead_lag":
        sample_count = int(safe_float(features.get("cross_sample_count")))
        lead_r1 = safe_float(features.get("cross_strength_r1"))
        lead_r3 = safe_float(features.get("cross_strength_r3"))
        breadth_r1 = safe_float(features.get("cross_breadth_r1"))
        pair_r1 = safe_float(features.get("pair_norm_r1"))
        volume_ratio = safe_float(features.get("volume_ratio_12"), 1.0)
        details.update(
            {
                "cross_sample_count": sample_count,
                "cross_strength_r1": round(lead_r1, 4),
                "cross_strength_r3": round(lead_r3, 4),
                "cross_breadth_r1": round(breadth_r1, 4),
                "pair_norm_r1": round(pair_r1, 4),
                "strength_pips": round(max(abs(lead_r1), abs(lead_r3)) * max(0.1, safe_float(features["m1_atr14_pips"])), 3),
            }
        )
        if sample_count < int(p["min_cross_samples"]):
            return RawSetup(None, "insufficient_cross_section", details)
        if volume_ratio < safe_float(p["min_volume_ratio"]):
            return RawSetup(None, "volume_filter", details)
        if (
            lead_r1 >= p["min_lead_r1"]
            and lead_r3 >= p["min_lead_r3"]
            and breadth_r1 >= p["min_breadth"]
            and abs(pair_r1) <= p["max_pair_r1_abs"]
        ):
            direction = "buy"
        elif (
            lead_r1 <= -p["min_lead_r1"]
            and lead_r3 <= -p["min_lead_r3"]
            and breadth_r1 <= -p["min_breadth"]
            and abs(pair_r1) <= p["max_pair_r1_abs"]
        ):
            direction = "sell"

    elif lane.family == "spread_compression_momentum":
        volume_ratio = safe_float(features.get("volume_ratio_12"), 1.0)
        spread_ratio = safe_float(features.get("spread_ratio_12"), 1.0)
        spread_drop = safe_float(features.get("spread_drop_3_pips"))
        candle_range = max(0.0, highs[-1] - lows[-1])
        close_location = (closes[-1] - lows[-1]) / max(candle_range, 1e-12)
        details.update(
            {
                "close_location": round(close_location, 4),
                "strength_pips": round(max(abs(r3), abs(r5)), 3),
            }
        )
        if (
            r1 >= p["min_r1"]
            and r3 >= p["min_r3"]
            and volume_ratio >= p["min_volume_ratio"]
            and spread_ratio <= p["max_spread_ratio"]
            and spread_drop >= p["min_spread_drop"]
            and close_location >= p["min_close_extreme"]
        ):
            direction = "buy"
        elif (
            r1 <= -p["min_r1"]
            and r3 <= -p["min_r3"]
            and volume_ratio >= p["min_volume_ratio"]
            and spread_ratio <= p["max_spread_ratio"]
            and spread_drop >= p["min_spread_drop"]
            and close_location <= 1.0 - p["min_close_extreme"]
        ):
            direction = "sell"

    elif lane.family == "kama_adaptive_trend":
        period = int(p["period"])
        if len(closes) < period + 2:
            return RawSetup(None, "insufficient_kama_candles", details)
        window = closes[-period:]
        efficiency = _efficiency_ratio(window)
        slow = ema_series(closes, period)
        fast = ema_series(closes, max(3, period // 3))
        atr = max(0.1, safe_float(features["m1_atr14_pips"]))
        gap_atr = (fast[-1] - slow[-1]) / pip / atr
        details.update({"adaptive_efficiency": round(efficiency, 4), "adaptive_gap_atr": round(gap_atr, 4)})
        if efficiency >= p["min_efficiency"] and gap_atr >= p["min_gap_atr"] and r1 >= p["min_r1"]:
            direction = "buy"
        elif efficiency >= p["min_efficiency"] and gap_atr <= -p["min_gap_atr"] and r1 <= -p["min_r1"]:
            direction = "sell"

    elif lane.family == "cci_reversion":
        period = int(p["period"])
        if len(closes) < period + 2:
            return RawSetup(None, "insufficient_cci_candles", details)
        typical = [(highs[i] + lows[i] + closes[i]) / 3.0 for i in range(len(closes))]
        def cci_at(end: int) -> float:
            sample = typical[end - period:end]
            mean = statistics.fmean(sample)
            mean_dev = statistics.fmean(abs(value - mean) for value in sample)
            return 0.0 if mean_dev <= 0.0 else (typical[end - 1] - mean) / (0.015 * mean_dev)
        previous_cci = cci_at(len(typical) - 1)
        current_cci = cci_at(len(typical))
        details.update({"previous_cci": round(previous_cci, 2), "cci": round(current_cci, 2)})
        if previous_cci <= -p["entry"] and current_cci > -p["exit_cross"] and r1 >= p["min_reversal_pips"]:
            direction = "buy"
        elif previous_cci >= p["entry"] and current_cci < p["exit_cross"] and r1 <= -p["min_reversal_pips"]:
            direction = "sell"

    elif lane.family == "breakout_retest":
        lookback = int(p["lookback"])
        if len(closes) < lookback + 2:
            return RawSetup(None, "insufficient_retest_candles", details)
        prior_high = max(highs[-lookback - 2 : -2])
        prior_low = min(lows[-lookback - 2 : -2])
        tolerance = safe_float(p["touch_tolerance_pips"]) * pip
        reclaim = safe_float(p["min_reclaim_pips"]) * pip
        details.update({"retest_high": round(prior_high, 6), "retest_low": round(prior_low, 6)})
        if lows[-1] <= prior_high + tolerance and closes[-1] >= prior_high + reclaim and m5_r3 >= p["min_m5_r3"]:
            direction = "buy"
        elif highs[-1] >= prior_low - tolerance and closes[-1] <= prior_low - reclaim and m5_r3 <= -p["min_m5_r3"]:
            direction = "sell"

    elif lane.family == "volume_climax_reversal":
        lookback = int(p["lookback"])
        if len(volumes) < lookback + 1:
            return RawSetup(None, "insufficient_volume_candles", details)
        baseline_volume = statistics.median(volumes[-lookback - 1 : -1])
        if baseline_volume <= 0.0:
            return RawSetup(None, "volume_unavailable", details)
        volume_ratio = safe_float(volumes[-1]) / baseline_volume
        candle_range = max(0.0, highs[-1] - lows[-1])
        body = closes[-1] - opens[-1]
        lower_tail = min(opens[-1], closes[-1]) - lows[-1]
        upper_tail = highs[-1] - max(opens[-1], closes[-1])
        atr_price = max(0.1, safe_float(features["m1_atr14_pips"])) * pip
        lower_tail_atr = lower_tail / atr_price
        upper_tail_atr = upper_tail / atr_price
        close_location = (closes[-1] - lows[-1]) / max(candle_range, 1e-12)
        details.update({"volume_ratio": round(volume_ratio, 4), "lower_tail_atr": round(lower_tail_atr, 4), "upper_tail_atr": round(upper_tail_atr, 4)})
        if volume_ratio >= p["min_volume_ratio"] and lower_tail_atr >= p["min_tail_atr"] and body > 0.0 and close_location <= p["long_pos_max"]:
            direction = "buy"
        elif volume_ratio >= p["min_volume_ratio"] and upper_tail_atr >= p["min_tail_atr"] and body < 0.0 and close_location >= p["short_pos_min"]:
            direction = "sell"

    elif lane.family == "spread_mean_reversion":
        spread_ratio = safe_float(features.get("spread_ratio_12"), 1.0)
        spread_drop = safe_float(features.get("spread_drop_3_pips"))
        if abs(r3) > p["max_abs_r3"]:
            return RawSetup(None, "spread_reversion_trend_filter", details)
        details.update({"spread_ratio": round(spread_ratio, 4), "spread_drop_3_pips": round(spread_drop, 3)})
        if spread_ratio >= p["min_spread_ratio"] and spread_drop >= p["min_spread_drop"] and pos20 <= p["long_pos_max"] and r1 >= 0.0:
            direction = "buy"
        elif spread_ratio >= p["min_spread_ratio"] and spread_drop >= p["min_spread_drop"] and pos20 >= p["short_pos_min"] and r1 <= 0.0:
            direction = "sell"

    elif lane.family == "volatility_shock_fade":
        lookback = int(p["lookback"])
        if len(closes) < lookback + 1:
            return RawSetup(None, "insufficient_shock_candles", details)
        candle_range = max(0.0, highs[-1] - lows[-1])
        atr_price = max(0.1, safe_float(features["m1_atr14_pips"])) * pip
        range_atr = candle_range / atr_price
        m5_trend_atr = m5_r3 / max(0.1, safe_float(features["m5_atr14_pips"]))
        close_location = (closes[-1] - lows[-1]) / max(candle_range, 1e-12)
        details.update({"shock_range_atr": round(range_atr, 4), "m5_trend_atr": round(m5_trend_atr, 4), "close_location": round(close_location, 4)})
        if range_atr >= p["min_range_atr"] and abs(m5_trend_atr) <= p["max_m5_trend_atr"]:
            if close_location <= 0.25 and r1 >= p["min_reversal_pips"]:
                direction = "buy"
            elif close_location >= 0.75 and r1 <= -p["min_reversal_pips"]:
                direction = "sell"

    elif lane.family == "session_range_breakout":
        lookback = int(p["lookback"])
        if len(highs) < lookback + 1:
            return RawSetup(None, "insufficient_session_range_candles", details)
        upper = max(highs[-lookback - 1 : -1])
        lower = min(lows[-lookback - 1 : -1])
        buffer = safe_float(p["buffer_pips"]) * pip
        volume_ratio = safe_float(features.get("volume_ratio_30"), 1.0)
        details.update({"session_upper": round(upper, 6), "session_lower": round(lower, 6), "volume_ratio_30": round(volume_ratio, 4)})
        if closes[-1] > upper + buffer and r1 >= p["min_r1"] and volume_ratio >= p["min_volume_ratio"]:
            direction = "buy"
        elif closes[-1] < lower - buffer and r1 <= -p["min_r1"] and volume_ratio >= p["min_volume_ratio"]:
            direction = "sell"

    elif lane.family == "micro_channel_break":
        lookback = int(p["lookback"])
        if len(closes) < lookback + 2:
            return RawSetup(None, "insufficient_channel_candles", details)
        slope_pips, r_squared = _linear_regression_stats(closes[-lookback - 1 : -1], pip)
        channel_high = max(highs[-lookback - 1 : -1])
        channel_low = min(lows[-lookback - 1 : -1])
        buffer = safe_float(p["break_buffer_pips"]) * pip
        details.update({"channel_slope_pips": round(slope_pips, 4), "channel_r2": round(r_squared, 4)})
        if slope_pips >= p["min_slope_pips"] and closes[-1] > channel_high + buffer and r1 >= p["min_r1"]:
            direction = "buy"
        elif slope_pips <= -p["min_slope_pips"] and closes[-1] < channel_low - buffer and r1 <= -p["min_r1"]:
            direction = "sell"

    elif lane.family == "trend_momentum_confluence":
        m15 = features.get("m15_closes") or []
        h1 = features.get("h1_closes") or []
        if len(m15) < 21 or len(h1) < 21:
            return RawSetup(None, "insufficient_confluence_candles", details)
        rsi_values = rsi_series(closes)
        rsi_value = rsi_values[-1] if rsi_values else None
        histogram = macd_histogram(closes)
        votes: list[tuple[str, int]] = []

        def trend_vote(name: str, value: float, neutral: float = 0.0) -> None:
            if value > neutral:
                votes.append((name, 1))
            elif value < neutral:
                votes.append((name, -1))

        trend_vote("m1_ema", ema_series(closes, 8)[-1] - ema_series(closes, 21)[-1])
        trend_vote("m5_ema", ema_series(m5_closes, 8)[-1] - ema_series(m5_closes, 21)[-1])
        trend_vote("m15_ema", ema_series(m15, 5)[-1] - ema_series(m15, 13)[-1])
        trend_vote("h1_ema", ema_series(h1, 5)[-1] - ema_series(h1, 13)[-1])
        trend_vote("macd_hist", histogram[-1] if histogram else 0.0)
        trend_vote("macd_slope", histogram[-1] - histogram[-2] if len(histogram) > 1 else 0.0)
        if rsi_value is not None:
            trend_vote("rsi_50", rsi_value, 50.0)
        trend_vote("r3", r3)
        buy_votes = sum(value > 0 for _, value in votes)
        sell_votes = sum(value < 0 for _, value in votes)
        details.update(
            {
                "confluence_votes": {name: value for name, value in votes},
                "buy_votes": buy_votes,
                "sell_votes": sell_votes,
                "vote_margin": abs(buy_votes - sell_votes),
                "rsi14": None if rsi_value is None else round(rsi_value, 2),
                "strength_pips": round(max(abs(r3), abs(r5)), 3),
            }
        )
        volume_ratio = safe_float(features.get("volume_ratio_12"), 1.0)
        if (
            buy_votes >= p["min_votes"]
            and buy_votes - sell_votes >= p["min_margin"]
            and r3 >= p["min_r3"]
            and rsi_value is not None
            and rsi_value <= p["max_rsi"]
            and volume_ratio >= p["min_volume_ratio"]
        ):
            direction = "buy"
        elif (
            sell_votes >= p["min_votes"]
            and sell_votes - buy_votes >= p["min_margin"]
            and r3 <= -p["min_r3"]
            and rsi_value is not None
            and rsi_value >= 100.0 - p["max_rsi"]
            and volume_ratio >= p["min_volume_ratio"]
        ):
            direction = "sell"

    elif lane.family == "breakout_volume_confluence":
        lookback = int(p["lookback"])
        if len(closes) < lookback + 1:
            return RawSetup(None, "insufficient_breakout_confluence_candles", details)
        prior_high = max(highs[-lookback - 1 : -1])
        prior_low = min(lows[-lookback - 1 : -1])
        buffer = safe_float(p["buffer_pips"]) * pip
        breakout_direction = "buy" if closes[-1] > prior_high + buffer else "sell" if closes[-1] < prior_low - buffer else ""
        candle_range = max(1e-12, highs[-1] - lows[-1])
        body = closes[-1] - opens[-1]
        close_location = (closes[-1] - lows[-1]) / candle_range
        range_atr = candle_range / (max(0.1, safe_float(features["m1_atr14_pips"])) * pip)
        volume_ratio = safe_float(features.get("volume_ratio_30"), 1.0)
        spread_ratio = safe_float(features.get("spread_ratio_12"), 1.0)
        votes = 0
        if breakout_direction:
            sign = 1.0 if breakout_direction == "buy" else -1.0
            votes += 1
            votes += int(volume_ratio >= p["min_volume_ratio"])
            votes += int(range_atr >= p["min_range_atr"])
            votes += int(body * sign > 0.0)
            votes += int(close_location >= 0.70 if sign > 0 else close_location <= 0.30)
            votes += int(m5_r3 * sign >= p["min_m5_r3"])
            votes += int(r1 * sign > 0.0)
            votes += int(spread_ratio <= p["max_spread_ratio"])
        details.update(
            {
                "breakout_direction": breakout_direction,
                "confluence_votes": votes,
                "prior_high": round(prior_high, 6),
                "prior_low": round(prior_low, 6),
                "range_atr": round(range_atr, 4),
                "close_location": round(close_location, 4),
                "volume_ratio": round(volume_ratio, 4),
                "strength_pips": round(max(abs(r1), abs(r3), range_atr * safe_float(features["m1_atr14_pips"])), 3),
            }
        )
        if breakout_direction and votes >= p["min_votes"]:
            direction = breakout_direction

    elif lane.family == "oscillator_reversion_confluence":
        if len(closes) < 24:
            return RawSetup(None, "insufficient_oscillator_confluence_candles", details)
        mean = statistics.fmean(closes[-20:])
        deviation = statistics.pstdev(closes[-20:])
        z_score = 0.0 if deviation <= 1e-12 else (closes[-1] - mean) / deviation
        rsi_values = rsi_series(closes)
        rsi_value = rsi_values[-1] if rsi_values else None
        stochastic_k, _ = _stochastic_series(highs, lows, closes, 14, 3)
        stochastic_value = stochastic_k[-1] if stochastic_k else None
        typical = [(highs[index] + lows[index] + closes[index]) / 3.0 for index in range(len(closes))]
        typical_sample = typical[-20:]
        typical_mean = statistics.fmean(typical_sample)
        mean_deviation = statistics.fmean(abs(value - typical_mean) for value in typical_sample)
        cci_value = 0.0 if mean_deviation <= 1e-12 else (typical[-1] - typical_mean) / (0.015 * mean_deviation)
        long_votes = int(z_score <= -p["z"])
        short_votes = int(z_score >= p["z"])
        if rsi_value is not None:
            long_votes += int(rsi_value <= p["rsi_low"])
            short_votes += int(rsi_value >= p["rsi_high"])
        if stochastic_value is not None:
            long_votes += int(stochastic_value <= p["stochastic_low"])
            short_votes += int(stochastic_value >= p["stochastic_high"])
        long_votes += int(cci_value <= -p["cci"])
        short_votes += int(cci_value >= p["cci"])
        long_votes += int(pos20 <= 0.30)
        short_votes += int(pos20 >= 0.70)
        long_votes += int(r1 >= p["min_reversal_pips"])
        short_votes += int(r1 <= -p["min_reversal_pips"])
        trend_atr = abs(m5_r3) / max(0.1, safe_float(features["m5_atr14_pips"]))
        details.update(
            {
                "bollinger_z": round(z_score, 4),
                "rsi14": None if rsi_value is None else round(rsi_value, 2),
                "stochastic_k": None if stochastic_value is None else round(stochastic_value, 2),
                "cci": round(cci_value, 2),
                "long_votes": long_votes,
                "short_votes": short_votes,
                "trend_atr": round(trend_atr, 4),
                "strength_pips": round(max(abs(r1), abs(z_score) * safe_float(features["m1_atr14_pips"])), 3),
            }
        )
        if trend_atr <= p["max_trend_atr"] and long_votes >= p["min_votes"] and long_votes > short_votes:
            direction = "buy"
        elif trend_atr <= p["max_trend_atr"] and short_votes >= p["min_votes"] and short_votes > long_votes:
            direction = "sell"

    elif lane.family == "cross_market_confluence":
        sample_count = int(safe_float(features.get("cross_sample_count")))
        if sample_count < int(p["min_cross_samples"]):
            return RawSetup(None, "insufficient_cross_market_samples", details)
        strength_r1 = safe_float(features.get("cross_strength_r1"))
        strength_r3 = safe_float(features.get("cross_strength_r3"))
        strength_r5 = safe_float(features.get("cross_strength_r5"))
        breadth = safe_float(features.get("cross_breadth_r3"))
        pair_r1 = safe_float(features.get("pair_norm_r1"))
        pair_r3 = safe_float(features.get("pair_norm_r3"))
        residual = safe_float(features.get("relative_residual_r3"))
        sources = (strength_r1, strength_r3, strength_r5, breadth, pair_r1, pair_r3, residual)
        buy_votes = sum(value > 0.0 for value in sources)
        sell_votes = sum(value < 0.0 for value in sources)
        details.update(
            {
                "cross_sample_count": sample_count,
                "cross_sources": [round(value, 4) for value in sources],
                "buy_votes": buy_votes,
                "sell_votes": sell_votes,
                "vote_margin": abs(buy_votes - sell_votes),
                "strength_pips": round(max(abs(r3), abs(strength_r3) * safe_float(features["m1_atr14_pips"])), 3),
            }
        )
        if (
            buy_votes >= p["min_votes"]
            and buy_votes - sell_votes >= p["min_margin"]
            and strength_r3 >= p["min_strength"]
            and breadth >= p["min_breadth"]
            and pair_r3 >= p["min_pair_r3"]
        ):
            direction = "buy"
        elif (
            sell_votes >= p["min_votes"]
            and sell_votes - buy_votes >= p["min_margin"]
            and strength_r3 <= -p["min_strength"]
            and breadth <= -p["min_breadth"]
            and pair_r3 <= -p["min_pair_r3"]
        ):
            direction = "sell"

    elif lane.family == "kalman_local_trend":
        period = int(p["period"])
        result = _kalman_local_linear(
            closes[-period:],
            pip,
            safe_float(p["process_ratio"]),
            safe_float(p["measurement_ratio"]),
        )
        if result is None:
            return RawSetup(None, "insufficient_kalman_history", details)
        forecast_pips, trend_pips, innovation_z, filtered_level = result
        details.update(
            {
                "kalman_filtered_level": round(filtered_level, 8),
                "kalman_trend_pips": round(trend_pips, 4),
                "kalman_innovation_z": round(innovation_z, 4),
                "expected_signed_move_pips": round(forecast_pips, 4),
                "strength_pips": round(abs(forecast_pips), 4),
                "theory": "kalman_local_linear_state",
            }
        )
        if abs(innovation_z) <= p["max_innovation_z"]:
            if forecast_pips >= p["min_forecast_pips"] and trend_pips >= p["min_trend_pips"]:
                direction = "buy"
            elif forecast_pips <= -p["min_forecast_pips"] and trend_pips <= -p["min_trend_pips"]:
                direction = "sell"

    elif lane.family == "markov_sign_transition":
        lookback = int(p["lookback"])
        result = _markov_sign_probability(
            closes[-lookback:],
            int(p["order"]),
            safe_float(p["laplace"]),
        )
        if result is None:
            return RawSetup(None, "insufficient_markov_history", details)
        probability_up, transition_count, state = result
        median_move = statistics.median(abs(value) for value in _pip_returns(closes[-lookback:], pip))
        expected_pips = (2.0 * probability_up - 1.0) * median_move
        edge = abs(probability_up - 0.5)
        details.update(
            {
                "markov_state": state,
                "markov_transition_count": transition_count,
                "probability_up": round(probability_up, 6),
                "probability_down": round(1.0 - probability_up, 6),
                "markov_probability_edge": round(edge, 6),
                "expected_signed_move_pips": round(expected_pips, 4),
                "strength_pips": round(abs(expected_pips), 4),
                "theory": "finite_state_markov_transition",
            }
        )
        if transition_count >= int(p["min_transitions"]) and edge >= p["min_probability_edge"]:
            direction = "buy" if probability_up > 0.5 else "sell"

    elif lane.family == "online_ar_forecast":
        period = int(p["period"])
        result = _online_ar_prediction(
            closes[-period:],
            pip,
            int(p["lags"]),
            safe_float(p["ridge"]),
        )
        if result is None:
            return RawSetup(None, "insufficient_online_ar_history", details)
        forecast_pips, r_squared, rows = result
        details.update(
            {
                "ar_lags": int(p["lags"]),
                "ar_rows": rows,
                "ar_r2": round(r_squared, 6),
                "expected_signed_move_pips": round(forecast_pips, 4),
                "strength_pips": round(abs(forecast_pips), 4),
                "theory": "ridge_regularized_autoregression",
            }
        )
        if rows >= int(p["min_rows"]) and r_squared >= p["min_r2"]:
            if forecast_pips >= p["min_forecast_pips"]:
                direction = "buy"
            elif forecast_pips <= -p["min_forecast_pips"]:
                direction = "sell"

    elif lane.family == "variance_ratio_regime":
        period = int(p["period"])
        returns = _pip_returns(closes[-period:], pip)
        ratio = _variance_ratio(returns, int(p["aggregation"]))
        if ratio is None:
            return RawSetup(None, "insufficient_variance_ratio_history", details)
        regime = "trend" if ratio >= p["trend_ratio"] else "reversion" if ratio <= p["reversion_ratio"] else "random_walk"
        expected_pips = 0.0
        if regime == "trend" and abs(r5) >= p["min_move_pips"]:
            direction = "buy" if r5 > 0.0 else "sell"
            expected_pips = r5 / 5.0
        elif (
            regime == "reversion"
            and abs(r3) >= p["min_move_pips"]
            and abs(r1) >= p["min_reversal_pips"]
            and r1 * r3 < 0.0
        ):
            direction = "buy" if r1 > 0.0 else "sell"
            expected_pips = r1
        details.update(
            {
                "variance_ratio": round(ratio, 6),
                "variance_ratio_q": int(p["aggregation"]),
                "variance_ratio_regime": regime,
                "expected_signed_move_pips": round(expected_pips, 4),
                "strength_pips": round(max(abs(r3), abs(r5)), 4),
                "theory": "lo_mackinlay_variance_ratio",
            }
        )

    elif lane.family == "permutation_entropy_momentum":
        period = int(p["period"])
        result = _permutation_entropy(closes[-period:], int(p["embedding"]))
        if result is None:
            return RawSetup(None, "insufficient_permutation_entropy_history", details)
        entropy, dominant_fraction, pattern_count = result
        expected_pips = r5 / 5.0
        details.update(
            {
                "permutation_entropy": round(entropy, 6),
                "permutation_dominant_fraction": round(dominant_fraction, 6),
                "permutation_pattern_count": pattern_count,
                "expected_signed_move_pips": round(expected_pips, 4),
                "strength_pips": round(abs(r5), 4),
                "theory": "bandt_pompe_permutation_entropy",
            }
        )
        if entropy <= p["max_entropy"] and abs(r5) >= p["min_move_pips"]:
            direction = "buy" if r5 > 0.0 else "sell"

    elif lane.family == "cusum_breakout":
        period = int(p["period"])
        result = _cusum_scores(_pip_returns(closes[-period:], pip), safe_float(p["drift_z"]))
        if result is None:
            return RawSetup(None, "insufficient_cusum_history", details)
        positive, negative, last_z = result
        expected_pips = r3 / 3.0
        details.update(
            {
                "cusum_positive": round(positive, 4),
                "cusum_negative": round(negative, 4),
                "cusum_last_z": round(last_z, 4),
                "expected_signed_move_pips": round(expected_pips, 4),
                "strength_pips": round(max(abs(r3), abs(r5)), 4),
                "theory": "page_two_sided_cusum",
            }
        )
        if positive >= p["threshold_z"] and last_z >= p["min_last_z"]:
            direction = "buy"
        elif negative >= p["threshold_z"] and last_z <= -p["min_last_z"]:
            direction = "sell"

    elif lane.family == "har_volatility_momentum":
        period = int(p["period"])
        result = _har_volatility_forecast(
            _pip_returns(closes[-period:], pip),
            int(p["short_window"]),
            int(p["medium_window"]),
            int(p["long_window"]),
            safe_float(p["ridge"]),
        )
        if result is None:
            return RawSetup(None, "insufficient_har_history", details)
        forecast_volatility, volatility_ratio, rows = result
        expected_pips = math.copysign(forecast_volatility, r3) if r3 else 0.0
        details.update(
            {
                "har_forecast_volatility_pips": round(forecast_volatility, 4),
                "har_forecast_ratio": round(volatility_ratio, 6),
                "har_rows": rows,
                "expected_signed_move_pips": round(expected_pips, 4),
                "strength_pips": round(max(abs(r3), forecast_volatility), 4),
                "theory": "corsi_har_realized_volatility",
            }
        )
        if volatility_ratio >= p["min_forecast_ratio"] and abs(r3) >= p["min_move_pips"]:
            direction = "buy" if r3 > 0.0 else "sell"

    elif lane.family == "bipower_jump_reversal":
        period = int(p["period"])
        result = _bipower_jump_stats(_pip_returns(closes[-period:], pip))
        if result is None:
            return RawSetup(None, "insufficient_bipower_history", details)
        jump_z, jump_share, jump_return, reversal_return, continuous_volatility = result
        details.update(
            {
                "bipower_jump_z": round(jump_z, 4),
                "bipower_jump_share": round(jump_share, 6),
                "bipower_jump_return_pips": round(jump_return, 4),
                "bipower_reversal_return_pips": round(reversal_return, 4),
                "bipower_continuous_volatility_pips": round(continuous_volatility, 4),
                "expected_signed_move_pips": round(reversal_return, 4),
                "strength_pips": round(abs(jump_return), 4),
                "theory": "barndorff_nielsen_shephard_bipower_jump",
            }
        )
        if (
            jump_z >= p["min_jump_z"]
            and jump_share >= p["min_jump_share"]
            and abs(reversal_return) >= p["min_reversal_pips"]
            and jump_return * reversal_return < 0.0
        ):
            direction = "buy" if reversal_return > 0.0 else "sell"

    elif lane.family == "garch_volatility_breakout":
        period = int(p["period"])
        result = _garch_volatility_forecast(
            _pip_returns(closes[-period:], pip),
            safe_float(p["alpha"]),
            safe_float(p["beta"]),
        )
        if result is None:
            return RawSetup(None, "insufficient_garch_history", details)
        forecast_volatility, volatility_ratio = result
        expected_pips = math.copysign(forecast_volatility, r3) if r3 else 0.0
        details.update(
            {
                "garch_forecast_volatility_pips": round(forecast_volatility, 4),
                "garch_volatility_ratio": round(volatility_ratio, 6),
                "expected_signed_move_pips": round(expected_pips, 4),
                "strength_pips": round(max(abs(r3), forecast_volatility), 4),
                "theory": "bollerslev_garch_1_1_filter",
            }
        )
        if volatility_ratio >= p["min_volatility_ratio"] and abs(r3) >= p["min_move_pips"]:
            direction = "buy" if r3 > 0.0 else "sell"

    elif lane.family == "hurst_regime_forecast":
        period = int(p["period"])
        result = _rescaled_range_hurst(_pip_returns(closes[-period:], pip))
        if result is None:
            return RawSetup(None, "insufficient_hurst_history", details)
        hurst, scale_count = result
        regime = "persistent" if hurst >= p["trend_hurst"] else "antipersistent" if hurst <= p["reversion_hurst"] else "random"
        expected_pips = 0.0
        if regime == "persistent" and abs(r5) >= p["min_move_pips"]:
            direction = "buy" if r5 > 0.0 else "sell"
            expected_pips = r5 / 5.0
        elif (
            regime == "antipersistent"
            and abs(r3) >= p["min_move_pips"]
            and abs(r1) >= p["min_reversal_pips"]
            and r1 * r3 < 0.0
        ):
            direction = "buy" if r1 > 0.0 else "sell"
            expected_pips = r1
        details.update(
            {
                "hurst_exponent": round(hurst, 6),
                "hurst_scale_count": scale_count,
                "hurst_regime": regime,
                "expected_signed_move_pips": round(expected_pips, 4),
                "strength_pips": round(max(abs(r3), abs(r5)), 4),
                "theory": "hurst_rescaled_range",
            }
        )

    elif lane.family == "theil_sen_trend":
        period = int(p["period"])
        result = _theil_sen_stats(closes[-period:], pip)
        if result is None:
            return RawSetup(None, "insufficient_theil_sen_history", details)
        slope_pips, residual_pips, residual_ratio = result
        details.update(
            {
                "theil_sen_slope_pips": round(slope_pips, 6),
                "theil_sen_residual_pips": round(residual_pips, 4),
                "theil_sen_residual_to_move": round(residual_ratio, 6),
                "expected_signed_move_pips": round(slope_pips, 4),
                "strength_pips": round(abs(slope_pips) * min(5, period), 4),
                "theory": "sen_kendall_robust_slope",
            }
        )
        if abs(slope_pips) >= p["min_slope_pips"] and residual_ratio <= p["max_residual_to_move"]:
            direction = "buy" if slope_pips > 0.0 else "sell"

    elif lane.family == "vwap_deviation_reversion":
        period = int(p["period"])
        tick_vwap = _tick_vwap(
            highs[-period:],
            lows[-period:],
            closes[-period:],
            volumes[-period:],
        )
        if tick_vwap is None:
            return RawSetup(None, "insufficient_tick_vwap_history", details)
        deviation_pips = (closes[-1] - tick_vwap) / pip
        atr = max(0.1, safe_float(features.get("m1_atr14_pips")))
        deviation_atr = deviation_pips / atr
        details.update(
            {
                "tick_volume_vwap": round(tick_vwap, 8),
                "tick_vwap_deviation_pips": round(deviation_pips, 4),
                "tick_vwap_deviation_atr": round(deviation_atr, 6),
                "expected_signed_move_pips": round(-deviation_pips, 4),
                "strength_pips": round(abs(deviation_pips), 4),
                "theory": "tick_volume_weighted_price_deviation",
                "volume_semantics": "oanda_tick_volume_not_exchange_volume",
            }
        )
        if safe_float(features.get("volume_ratio_12"), 1.0) >= p["min_volume_ratio"]:
            if deviation_atr <= -p["entry_atr"] and r1 >= p["min_reversal_pips"]:
                direction = "buy"
            elif deviation_atr >= p["entry_atr"] and r1 <= -p["min_reversal_pips"]:
                direction = "sell"

    elif lane.family == "ny_session_vwap_sell_reversion":
        signal_time = parse_rfc3339(str(features.get("candle_time") or ""))
        if signal_time is None:
            return RawSetup(None, "invalid_signal_time", details)
        expected_entry_time = signal_time + timedelta(minutes=1)
        local_entry_time = expected_entry_time.astimezone(NEW_YORK_TZ)
        local_hour = local_entry_time.hour
        details.update(
            {
                "new_york_entry_time": local_entry_time.isoformat(),
                "new_york_hour": local_hour,
                "session_start_hour": int(p["session_start_hour"]),
                "session_end_hour": int(p["session_end_hour"]),
                "target_horizon_sec": int(p["target_horizon_sec"]),
                "side_constraint": "sell_only",
                "theory": "tick_vwap_reversion_with_empirical_new_york_session_gate",
            }
        )
        if not int(p["session_start_hour"]) <= local_hour < int(p["session_end_hour"]):
            return RawSetup(None, "outside_new_york_session", details)
        period = int(p["period"])
        tick_vwap = _tick_vwap(
            highs[-period:],
            lows[-period:],
            closes[-period:],
            volumes[-period:],
        )
        if tick_vwap is None:
            return RawSetup(None, "insufficient_tick_vwap_history", details)
        deviation_pips = (closes[-1] - tick_vwap) / pip
        atr = max(0.1, safe_float(features.get("m1_atr14_pips")))
        deviation_atr = deviation_pips / atr
        details.update(
            {
                "tick_volume_vwap": round(tick_vwap, 8),
                "tick_vwap_deviation_pips": round(deviation_pips, 4),
                "tick_vwap_deviation_atr": round(deviation_atr, 6),
                "expected_signed_move_pips": round(-deviation_pips, 4),
                "strength_pips": round(abs(deviation_pips), 4),
                "volume_semantics": "oanda_tick_volume_not_exchange_volume",
            }
        )
        if (
            safe_float(features.get("volume_ratio_12"), 1.0) >= p["min_volume_ratio"]
            and deviation_atr >= p["entry_atr"]
            and r1 <= -p["min_reversal_pips"]
        ):
            direction = "sell"

    elif lane.family == "inverse_correlation_veto":
        votes = features.get("strategy_vote_features") or {}
        source_rows: list[dict[str, Any]] = []
        for source in INVERSE_VOTE_SOURCES:
            vote = safe_float(votes.get(f"accepted_{source}_vote"))
            activity = safe_float(votes.get(f"accepted_{source}_activity"))
            if activity < p["min_source_activity"] or abs(vote) <= 1e-12:
                continue
            source_rows.append({"family": source, "vote": round(vote, 4), "activity": round(activity, 4)})
        consensus = sum(safe_float(row["vote"]) for row in source_rows)
        details.update(
            {
                "inverse_sources": source_rows,
                "source_count": len(source_rows),
                "lagging_vote_consensus": round(consensus, 4),
                "vetoed_direction": "buy" if consensus > 0.0 else "sell" if consensus < 0.0 else "",
                "strength_pips": round(abs(consensus) * safe_float(features["m1_atr14_pips"]), 3),
                "shadow_only": True,
            }
        )
        if len(source_rows) < int(p["min_sources"]):
            return RawSetup(None, "inverse_veto_source_support", details)
        if abs(consensus) < p["min_vote_magnitude"]:
            return RawSetup(None, "inverse_veto_magnitude", details)
        direction = "sell" if consensus > 0.0 else "buy"

    elif lane.family == "signal_combination_rules":
        model_forecast = features.get("signal_combination_forecast") or {}
        if not model_forecast.get("ready"):
            return RawSetup(None, "combination_model_collecting", details)
        candidates = model_forecast.get("candidates") or [model_forecast]

        def combination_gate(candidate: dict[str, Any]) -> bool:
            return (
                bool(candidate.get("account_eligible", True))
                and int(safe_float(candidate.get("rule_size"))) >= int(p["min_rule_size"])
                and safe_float(candidate.get("train_support")) >= p["min_train_support"]
                and safe_float(candidate.get("holdout_support")) >= p["min_holdout_support"]
                and safe_float(candidate.get("holdout_lower_probability_edge")) >= p["min_lower_edge"]
                and safe_float(candidate.get("membership")) >= p["min_membership"]
                and safe_float(candidate.get("expected_net_pips")) >= p["min_expected_net_pips"]
            )

        forecast = next((candidate for candidate in candidates if combination_gate(candidate)), None)
        diagnostic_forecast = forecast or model_forecast
        details.update(
            {
                "combination_forecast": diagnostic_forecast,
                "rule_id": diagnostic_forecast.get("rule_id"),
                "rule_conditions": diagnostic_forecast.get("conditions") or [],
                "rule_condition_text": diagnostic_forecast.get("condition_text") or "",
                "rule_membership": safe_float(diagnostic_forecast.get("membership")),
                "rule_train_support": safe_float(diagnostic_forecast.get("train_support")),
                "rule_holdout_support": safe_float(diagnostic_forecast.get("holdout_support")),
                "rule_holdout_lower_edge": safe_float(diagnostic_forecast.get("holdout_lower_probability_edge")),
                "rule_holdout_brier": safe_float(diagnostic_forecast.get("holdout_brier")),
                "rule_account_eligible": bool(diagnostic_forecast.get("account_eligible", True)),
                "rule_shadow_reason": str(diagnostic_forecast.get("shadow_reason") or ""),
                "rule_horizon_sec": int(safe_float(diagnostic_forecast.get("horizon_sec"))),
                "strength_pips": abs(safe_float(diagnostic_forecast.get("expected_net_pips"))),
            }
        )
        if forecast is None:
            return RawSetup(None, "combination_rule_gate", details)
        forecast_direction = str(forecast.get("predicted_direction") or "")
        if forecast_direction not in {"buy", "sell"}:
            return RawSetup(None, "combination_direction_unavailable", details)
        direction = forecast_direction

    if direction is None:
        return RawSetup(None, "no_setup", details)
    details.setdefault("strength_pips", round(max(abs(r1), abs(r3), abs(r5)), 3))
    return RawSetup(direction, "setup", details)


def economic_gates(
    lane: LaneSpec,
    features: dict[str, Any],
    bid: float,
    ask: float,
    signal: dict[str, Any] | None = None,
) -> tuple[list[str], dict[str, float]]:
    p = lane.parameters
    pip = safe_float(features["pip"], 0.0001)
    spread = max(0.0, (ask - bid) / pip)
    atr = max(0.0, safe_float(features["m5_atr14_pips"]))
    impulse = max(
        abs(safe_float(features["r3_pips"])),
        abs(safe_float(features["r5_pips"])),
        abs(safe_float((signal or {}).get("strength_pips"))),
    )
    target = safe_float(p["stop_loss_pips"]) * safe_float(p["take_profit_r"])
    reward_to_spread = math.inf if spread <= 0.0 else target / spread
    signal_to_spread = math.inf if spread <= 0.0 else impulse / spread
    reasons: list[str] = []
    if spread > safe_float(p["max_spread_pips"]):
        reasons.append("spread_absolute")
    if atr <= 0.0 or spread > atr * safe_float(p["max_spread_atr_fraction"]):
        reasons.append("spread_vs_atr")
    if reward_to_spread < safe_float(p["min_reward_to_spread"]):
        reasons.append("reward_vs_spread")
    if signal_to_spread < safe_float(p["min_signal_to_spread"]):
        reasons.append("signal_vs_spread")
    metrics = {
        "spread_pips": round(spread, 3),
        "atr_pips": round(atr, 3),
        "reward_to_spread": round(reward_to_spread, 3),
        "signal_to_spread": round(signal_to_spread, 3),
    }
    if lane.family == "pattern_count_forecast":
        forecast = ((signal or {}).get("pattern_forecast") or {})
        sample_count = safe_float(forecast.get("combined_pattern_count"))
        direction_edge = safe_float(forecast.get("direction_edge"))
        movement_coefficient = safe_float(forecast.get("movement_coefficient"))
        if sample_count < safe_float(p["min_pattern_count"]):
            reasons.append("pattern_sample_count")
        if direction_edge < safe_float(p["min_direction_edge"]):
            reasons.append("pattern_direction_edge")
        if movement_coefficient < safe_float(p["min_movement_coefficient"]):
            reasons.append("pattern_movement_coefficient")
        metrics.update(
            {
                "pattern_sample_count": round(sample_count, 3),
                "pattern_direction_edge": round(direction_edge, 6),
                "pattern_movement_coefficient": round(movement_coefficient, 6),
            }
        )
    return reasons, metrics


def classify_miss(lane: LaneSpec, metrics: dict[str, float]) -> str:
    p = lane.parameters
    spread = safe_float(metrics.get("spread_pips"))
    atr = safe_float(metrics.get("atr_pips"))
    reward_ratio = safe_float(metrics.get("reward_to_spread"))
    signal_ratio = safe_float(metrics.get("signal_to_spread"))
    near = (
        spread <= 2.0 * safe_float(p["max_spread_pips"])
        and atr > 0.0
        and spread <= 2.0 * atr * safe_float(p["max_spread_atr_fraction"])
        and reward_ratio >= 0.5 * safe_float(p["min_reward_to_spread"])
        and signal_ratio >= 0.5 * safe_float(p["min_signal_to_spread"])
    )
    if lane.family == "pattern_count_forecast":
        near = (
            near
            and safe_float(metrics.get("pattern_sample_count")) >= 0.5 * safe_float(p["min_pattern_count"])
            and safe_float(metrics.get("pattern_direction_edge")) >= 0.5 * safe_float(p["min_direction_edge"])
            and safe_float(metrics.get("pattern_movement_coefficient"))
            >= 0.5 * safe_float(p["min_movement_coefficient"])
        )
    return "near_threshold" if near else "hard_reject"


def should_track_shadow_outcome(
    event_id: str,
    kind: str,
    miss_class: str,
    hard_reject_sample_rate: float,
    near_threshold_sample_rate: float | None = None,
) -> bool:
    """Retain all viable signals and a deterministic sample of blocked setups."""

    if kind == "signal":
        return True
    hard_rate = max(0.0, min(1.0, float(hard_reject_sample_rate)))
    near_rate = (
        max(0.0, min(1.0, float(near_threshold_sample_rate)))
        if near_threshold_sample_rate is not None
        else max(0.10, min(1.0, hard_rate * 10.0))
    )
    sample_rate = near_rate if miss_class == "near_threshold" else hard_rate
    if sample_rate <= 0.0:
        return False
    digest = hashlib.sha256(str(event_id).encode("utf-8")).digest()
    draw = int.from_bytes(digest[:8], "big") / float(2**64)
    return draw < sample_rate


def theoretical_pips(direction: str, pip: float, entry_bid: float, entry_ask: float, exit_bid: float, exit_ask: float) -> float:
    if direction == "buy":
        return (exit_bid - entry_ask) / pip
    return (entry_bid - exit_ask) / pip


def outcome_quote_rejection_reason(quote: Any) -> str:
    if quote is None:
        return "missing_quote"
    if not bool(getattr(quote, "tradeable", True)):
        return "quote_not_tradeable"
    quote_time = parse_rfc3339(str(getattr(quote, "time", "") or ""))
    if quote_time is None:
        return "quote_time_invalid"
    quote_age_sec = max(
        0.0,
        (datetime.now(timezone.utc) - quote_time).total_seconds(),
    )
    if quote_age_sec > OUTCOME_QUOTE_MAX_AGE_SEC:
        return "stale_quote"
    return ""


def merge_price_maps(*price_maps: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for prices in price_maps:
        for instrument, quote in prices.items():
            existing = merged.get(instrument)
            if existing is None:
                merged[instrument] = quote
                continue
            existing_time = parse_rfc3339(str(existing.time or ""))
            quote_time = parse_rfc3339(str(quote.time or ""))
            if existing_time is None or (quote_time is not None and quote_time >= existing_time):
                merged[instrument] = quote
    return merged


def execution_price_snapshot(
    stream: MultiPriceStream | None,
    cached_prices: dict[str, Any],
    snapshot_provider: Callable[[], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return the freshest execution diagnostic prices available.

    The strategy lab is deliberately allowed to run without its own streaming
    connection and fall back to OANDA REST snapshots.  In that mode the
    executor previously received no price provider at all, leaving candidate
    repricing and the research signal snapshot's quote coverage at zero even
    while the lab held fresh executable quotes.  Merge a live stream overlay
    when present, otherwise expose a copy of the current REST/cache view.  All
    downstream tradeability and quote-age checks remain authoritative.
    """

    snapshot_prices = snapshot_provider() if snapshot_provider is not None else {}
    stream_prices = stream.snapshot() if stream is not None else {}
    return merge_price_maps(cached_prices, snapshot_prices, stream_prices)


def merge_price_overlay(
    current: dict[str, Any],
    overlay: dict[str, Any],
    *,
    provider: str,
    observed_utc: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Merge a quote overlay and retain auditable observation provenance.

    Quote timestamps remain broker timestamps and the normal 30-second
    rejection gate remains authoritative. The helper only reports which
    newer quote objects were selected; it never rewrites timestamps or
    tradeability.
    """

    merged = merge_price_maps(current, overlay)
    selected = sorted(
        instrument
        for instrument, quote in overlay.items()
        if merged.get(instrument) is quote
    )
    source_counts: Counter[str] = Counter(
        str(getattr(merged[instrument], "source", "") or "unknown")
        for instrument in selected
    )
    return merged, {
        "provider": str(provider),
        "observed_utc": observed_utc or datetime.now(timezone.utc).isoformat(),
        "received": len(overlay),
        "selected": len(selected),
        "selected_instruments": selected,
        "selected_source_counts": dict(sorted(source_counts.items())),
        "freshness_threshold_sec": OUTCOME_QUOTE_MAX_AGE_SEC,
        "timestamps_preserved": True,
        "tradeability_preserved": True,
    }


def candle_snapshot(
    client: MarketDataClient,
    instruments: list[str],
    executor: ThreadPoolExecutor,
    include_m5: bool,
    m1_count: int = 120,
    include_daily: bool = False,
) -> tuple[dict[str, dict[str, list[dict[str, Any]]]], Counter[str]]:
    output: dict[str, dict[str, list[dict[str, Any]]]] = {}
    errors: Counter[str] = Counter()
    futures = {}
    for instrument in instruments:
        futures[executor.submit(client.complete_candles, instrument, "M1", m1_count)] = (instrument, "M1")
        if include_m5:
            futures[executor.submit(client.complete_candles, instrument, "M5", 360)] = (instrument, "M5")
            futures[executor.submit(client.complete_candles, instrument, "H1", 500)] = (instrument, "H1")
        if include_daily:
            futures[executor.submit(client.complete_candles, instrument, "D", 120)] = (instrument, "D")
    for future in as_completed(futures):
        instrument, granularity = futures[future]
        try:
            output.setdefault(instrument, {})[granularity] = future.result()
        except Exception as exc:
            errors[type(exc).__name__] += 1
    return output, errors


def merge_candle_history(
    existing: list[dict[str, Any]],
    incoming: list[dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    """Merge overlapping REST refreshes without discarding MA warmup history."""

    rows = {
        str(row.get("time") or ""): row
        for row in (*existing, *incoming)
        if str(row.get("time") or "")
    }
    ordered = [rows[key] for key in sorted(rows)]
    return ordered[-max(1, int(limit)) :]


def merge_candle_cache(
    cache: dict[str, dict[str, list[dict[str, Any]]]],
    update: dict[str, dict[str, list[dict[str, Any]]]],
) -> None:
    limits = {"M1": 5000, "M5": 500, "H1": 500, "D": 400}
    for instrument, granularities in update.items():
        target = cache.setdefault(instrument, {})
        for granularity, rows in granularities.items():
            target[granularity] = merge_candle_history(
                target.get(granularity) or [],
                rows,
                limits.get(granularity, 500),
            )


def selected_instruments(args: argparse.Namespace, token: str, account_id: str) -> list[str]:
    instruments = parse_instrument_list(args.instruments)
    if not instruments:
        instruments = tradeable_currency_instruments(token, account_id)
    if args.exclude_regex:
        pattern = re.compile(args.exclude_regex)
        instruments = [instrument for instrument in instruments if not pattern.search(instrument)]
    return sorted(dict.fromkeys(instruments))


def account_pip_sizes(client: MarketDataClient, account_id: str) -> dict[str, float]:
    payload = client.get(f"/v3/accounts/{account_id}/instruments", params={})
    pip_sizes: dict[str, float] = {}
    for instrument in payload.get("instruments") or []:
        name = str(instrument.get("name") or "")
        try:
            pip_location = int(instrument.get("pipLocation"))
        except (TypeError, ValueError):
            continue
        pip_size = 10.0**pip_location
        if name and 0.0 < pip_size <= 1.0:
            pip_sizes[name] = pip_size
    return pip_sizes


def next_refresh_epoch(delay_sec: float) -> float:
    now = time.time()
    return (math.floor(now / 60.0) + 1.0) * 60.0 + delay_sec


def slow_candle_refresh_due(
    candle_cache: dict[str, dict[str, list[dict[str, Any]]]],
    instruments: list[str],
    last_refresh_monotonic: float,
    interval_sec: float,
    *,
    now_monotonic: float | None = None,
) -> bool:
    if not candle_cache or any(
        "M5" not in candle_cache.get(instrument, {})
        or "H1" not in candle_cache.get(instrument, {})
        for instrument in instruments
    ):
        return True
    now = time.monotonic() if now_monotonic is None else now_monotonic
    return now - last_refresh_monotonic >= max(1.0, interval_sec)


def daily_candle_refresh_due(
    candle_cache: dict[str, dict[str, list[dict[str, Any]]]],
    instruments: list[str],
    last_refresh_monotonic: float,
    interval_sec: float,
    *,
    now_monotonic: float | None = None,
) -> bool:
    if not candle_cache or any(
        "D" not in candle_cache.get(instrument, {}) for instrument in instruments
    ):
        return True
    now = time.monotonic() if now_monotonic is None else now_monotonic
    return now - last_refresh_monotonic >= max(60.0, interval_sec)


def parse_horizons(value: str) -> list[float]:
    horizons = sorted({float(item) for item in re.split(r"[\s,]+", value.strip()) if item})
    if not horizons or horizons[0] <= 0.0:
        raise argparse.ArgumentTypeError("outcome horizons must be positive seconds")
    return horizons


def evaluate_cycle(
    lanes: list[LaneSpec],
    instruments: list[str],
    candles: dict[str, dict[str, list[dict[str, Any]]]],
    prices: dict[str, Any],
    pip_sizes: dict[str, float],
    horizons: list[float],
    log_path: Path,
    pending: list[dict[str, Any]],
    seen: set[tuple[str, str, str]],
    outcome_store: ShadowOutcomeStore | None = None,
    executor: PracticeExecutor | None = None,
    pattern_forecaster: PatternCountForecaster | None = None,
    timeframe_matrix: TimeframeHorizonMatrix | None = None,
    combination_model: SignalCombinationModel | None = None,
    combination_store: SignalCombinationStore | None = None,
    combination_pending: list[dict[str, Any]] | None = None,
    combination_seen: set[tuple[str, str]] | None = None,
    sma_filter_model: SmaSignalFilterModel | None = None,
    sma_filter_store: SmaSignalFilterStore | None = None,
    ma_grid_runtime: MaFeatureGridRuntime | None = None,
    price_metadata: dict[str, dict[str, Any]] | None = None,
    live_feature_snapshot_path: Path | None = None,
    price_provider: Callable[[], dict[str, Any]] | None = None,
    snapshot_price_provider: Callable[[], dict[str, Any]] | None = None,
    timeframe_feature_memo: dict[
        tuple[str, str],
        tuple[str, dict[str, Any]],
    ]
    | None = None,
    hard_reject_log_sample_rate: float = 0.05,
    hard_reject_outcome_sample_rate: float = 0.05,
    near_threshold_outcome_sample_rate: float = 0.50,
    shadow_outcome_horizon_mode: str = "all",
) -> None:
    cycle_started_monotonic = time.monotonic()
    quote_refresh_totals: Counter[str] = Counter()

    def refresh_cycle_prices(
        provider: Callable[[], dict[str, Any]] | None = None,
        *,
        provider_name: str = "price_provider",
        stage: str = "",
        audit: bool = False,
    ) -> dict[str, Any]:
        nonlocal prices
        selected_provider = provider or price_provider
        if selected_provider is None:
            quote_refresh_totals[f"{provider_name}_unavailable"] += 1
            return {
                "provider": provider_name,
                "stage": stage,
                "status": "provider_unavailable",
                "received": 0,
                "selected": 0,
            }
        started = time.monotonic()
        observed_utc = datetime.now(timezone.utc).isoformat()
        quote_refresh_totals[f"{provider_name}_attempts"] += 1
        try:
            latest = selected_provider()
        except Exception as exc:
            quote_refresh_totals[f"{provider_name}_errors"] += 1
            result = {
                "provider": provider_name,
                "stage": stage,
                "status": "error",
                "error": f"{type(exc).__name__}: {exc}"[:300],
                "received": 0,
                "selected": 0,
                "elapsed_sec": round(time.monotonic() - started, 3),
                "freshness_threshold_sec": OUTCOME_QUOTE_MAX_AGE_SEC,
                "fail_closed": True,
            }
            if audit:
                log_line(log_path, "lab_quote_overlay", **result)
            return result
        if latest:
            prices, provenance = merge_price_overlay(
                prices,
                latest,
                provider=provider_name,
                observed_utc=observed_utc,
            )
        else:
            provenance = {
                "provider": provider_name,
                "observed_utc": observed_utc,
                "received": 0,
                "selected": 0,
                "selected_instruments": [],
                "selected_source_counts": {},
                "freshness_threshold_sec": OUTCOME_QUOTE_MAX_AGE_SEC,
                "timestamps_preserved": True,
                "tradeability_preserved": True,
            }
        quote_refresh_totals[f"{provider_name}_successes"] += 1
        quote_refresh_totals[f"{provider_name}_received"] += int(
            provenance["received"]
        )
        quote_refresh_totals[f"{provider_name}_selected"] += int(
            provenance["selected"]
        )
        result = {
            **provenance,
            "stage": stage,
            "status": "ok",
            "elapsed_sec": round(time.monotonic() - started, 3),
            "fail_closed": True,
        }
        if audit:
            log_line(log_path, "lab_quote_overlay", **result)
        return result

    feature_cache: dict[str, dict[str, Any]] = {}
    timeframe_feature_cache: dict[str, dict[str, dict[str, Any]]] = {}
    feature_observation_clocks: dict[str, Any] = {
        "primary_calculated_utc_by_instrument": {},
        "timeframe_view_retrieved_utc_by_instrument": {},
    }
    accepted_candidates: list[dict[str, Any]] = []
    feature_errors: Counter[str] = Counter()
    for instrument in instruments:
        features, reason = build_features(
            instrument,
            candles.get(instrument) or {},
            pip_sizes.get(instrument),
        )
        if features is None:
            feature_errors[reason] += 1
        else:
            feature_cache[instrument] = features
            feature_observation_clocks["primary_calculated_utc_by_instrument"][instrument] = datetime.now(timezone.utc).isoformat()
    primary_feature_elapsed_sec = (
        time.monotonic() - cycle_started_monotonic
    )

    ma_grid_forecasts = 0
    ma_grid_timeframes: Counter[str] = Counter()
    ma_grid_quote_rejections: Counter[str] = Counter()
    ma_grid_top: list[dict[str, Any]] = []
    ma_grid_publish: dict[str, Any] = {
        "available": False,
        "received": 0,
        "accepted": 0,
        "rejected": 0,
    }
    ma_grid_request_sec = 0.0
    ma_grid_inference_sec = 0.0
    ma_grid_publish_sec = 0.0
    ma_grid_postprocess_sec = 0.0
    ma_grid_batch_stats: dict[str, Any] = {}
    if ma_grid_runtime is not None and ma_grid_runtime.ready:
        ma_stage_started = time.monotonic()
        refresh_cycle_prices(provider_name="stream", stage="ma_request")
        generated_epoch = time.time()
        ma_requests: dict[
            str,
            tuple[dict[str, Any], float, float],
        ] = {}
        for instrument, features in feature_cache.items():
            quote = prices.get(instrument)
            quote_rejection = outcome_quote_rejection_reason(quote)
            if quote_rejection:
                ma_grid_quote_rejections[quote_rejection] += 1
                continue
            ma_requests[instrument] = (
                features,
                safe_float(quote.bid),
                safe_float(quote.ask),
            )
        ma_grid_request_sec = time.monotonic() - ma_stage_started
        inference_started = time.monotonic()
        log_line(
            log_path,
            "lab_cycle_stage",
            stage="ma_inference_start",
            instruments=len(ma_requests),
        )
        ma_candidates = ma_grid_runtime.forecast_candidates_batch(
            ma_requests,
            generated_epoch=generated_epoch,
        )
        ma_grid_inference_sec = time.monotonic() - inference_started
        log_line(
            log_path,
            "lab_cycle_stage",
            stage="ma_inference_complete",
            candidates=len(ma_candidates),
            elapsed_sec=round(ma_grid_inference_sec, 3),
        )
        ma_grid_batch_stats = dict(ma_grid_runtime.last_batch_stats)
        ma_grid_forecasts = len(ma_candidates)
        publish_started = time.monotonic()
        log_line(
            log_path,
            "lab_cycle_stage",
            stage="ma_publish_start",
            candidates=len(ma_candidates),
        )
        if executor is not None and executor.signal_feed is not None:
            ma_grid_publish = executor.publish_research_forecasts(
                ma_candidates,
                "ma_feature_grid",
            )
        else:
            accepted_candidates.extend(ma_candidates)
        ma_grid_publish_sec = time.monotonic() - publish_started
        log_line(
            log_path,
            "lab_cycle_stage",
            stage="ma_publish_complete",
            accepted=int(ma_grid_publish.get("accepted") or 0),
            elapsed_sec=round(ma_grid_publish_sec, 3),
        )
        postprocess_started = time.monotonic()
        for candidate in ma_candidates:
            ma_grid_timeframes[
                str(candidate.get("input_timeframe") or "")
            ] += 1
            ma_grid_top.append(
                {
                    "instrument": candidate.get("instrument"),
                    "timeframe": candidate.get("input_timeframe"),
                    "direction": candidate.get("direction"),
                    "horizon_sec": candidate.get(
                        "signal_reference_horizon_sec"
                    ),
                    "probability_up": candidate.get("probability_up"),
                    "projected_net_pips": candidate.get(
                        "projected_net_pips"
                    ),
                }
            )
        ma_grid_top.sort(
            key=lambda row: abs(
                safe_float(row.get("projected_net_pips"))
            ),
            reverse=True,
        )
        ma_grid_postprocess_sec = time.monotonic() - postprocess_started
    ma_grid_elapsed_sec = time.monotonic() - cycle_started_monotonic

    for instrument in instruments:
        timeframe_feature_cache[instrument] = build_timeframe_feature_views(
            instrument,
            candles.get(instrument) or {},
            pip_sizes.get(instrument),
            cache=timeframe_feature_memo,
            primary_features=feature_cache.get(instrument),
        )
        feature_observation_clocks["timeframe_view_retrieved_utc_by_instrument"][instrument] = datetime.now(timezone.utc).isoformat()
    timeframe_feature_elapsed_sec = (
        time.monotonic() - cycle_started_monotonic
    )
    # The research-feed publish above can take longer than the strict quote
    # freshness window. Refresh from the read-only REST snapshot here so the
    # quote-dependent feature augmentations use current market state.
    snapshot_refresh_started = time.monotonic()
    log_line(log_path, "lab_cycle_stage", stage="snapshot_quote_refresh_start")
    refresh_cycle_prices(
        snapshot_price_provider,
        provider_name="rest_snapshot",
        stage="feature_augmentation",
    )
    log_line(
        log_path,
        "lab_cycle_stage",
        stage="snapshot_quote_refresh_complete",
        elapsed_sec=round(time.monotonic() - snapshot_refresh_started, 3),
    )
    feature_observation_clocks["quote_feature_capture_utc"] = datetime.now(timezone.utc).isoformat()
    feature_observation_clocks["quote_observed_utc_by_instrument"] = {
        instrument: str(getattr(quote, "time", "") or "")
        for instrument, quote in prices.items()
    }
    augment_market_microstructure_features(feature_cache, prices, price_metadata)
    augment_cross_sectional_features(feature_cache)
    available_timeframes = sorted(
        {
            timeframe
            for views in timeframe_feature_cache.values()
            for timeframe in views
        }
    )
    for timeframe in available_timeframes:
        scoped = {
            instrument: views[timeframe]
            for instrument, views in timeframe_feature_cache.items()
            if timeframe in views
        }
        augment_market_microstructure_features(scoped, prices, price_metadata)
        augment_cross_sectional_features(scoped)
    feature_observation_clocks["timeframe_observed_utc"] = datetime.now(timezone.utc).isoformat()
    augment_supervised_return_features(feature_cache, candles)
    pattern_cycle = augment_pattern_count_features(
        feature_cache,
        candles,
        pip_sizes,
        lanes,
        pattern_forecaster,
    )
    feature_observation_clocks["primary_observed_utc"] = datetime.now(timezone.utc).isoformat()

    # Pattern enrichment can itself exceed the strict quote-freshness window.
    # Refresh again at the consumption boundary so the persisted snapshot and
    # the lane evaluations see current executable quotes.  The downstream
    # 30-second rejection gate remains authoritative.
    final_quote_refresh_started = time.monotonic()
    log_line(log_path, "lab_cycle_stage", stage="final_quote_refresh_start")
    refresh_cycle_prices(
        snapshot_price_provider,
        provider_name="rest_snapshot",
        stage="feature_snapshot",
    )
    log_line(
        log_path,
        "lab_cycle_stage",
        stage="final_quote_refresh_complete",
        elapsed_sec=round(time.monotonic() - final_quote_refresh_started, 3),
    )
    cycle_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    snapshot_write_started = time.monotonic()
    log_line(log_path, "lab_cycle_stage", stage="feature_snapshot_write_start")
    live_feature_snapshot_instruments = write_live_model_feature_snapshot(
        live_feature_snapshot_path,
        cycle_id,
        feature_cache,
        prices,
        price_metadata,
        timeframe_feature_cache,
        observation_archive_root=DEFAULT_OBSERVATION_ARCHIVE_ROOT,
        observation_clocks=feature_observation_clocks,
    )
    snapshot_write_sec = time.monotonic() - snapshot_write_started
    log_line(
        log_path,
        "lab_cycle_stage",
        stage="feature_snapshot_write_complete",
        instruments=live_feature_snapshot_instruments,
        elapsed_sec=round(snapshot_write_sec, 3),
    )
    strategy_votes: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    combination_vectors: dict[str, dict[str, float]] = {}
    combination_prepared = False
    sma_vectors: dict[tuple[str, str], dict[str, float]] = {}
    sma_forecasts: dict[tuple[str, str], dict[str, Any]] = {}
    sma_registered = 0
    sma_prepare_started = time.monotonic()
    for instrument, features in feature_cache.items():
        quote = prices.get(instrument)
        if outcome_quote_rejection_reason(quote):
            continue
        spread_pips = (
            max(0.0, safe_float(quote.ask) - safe_float(quote.bid))
            / max(1e-12, safe_float(features.get("pip"), 0.0001))
        )
        for direction in ("buy", "sell"):
            key = (instrument, direction)
            sma_vectors[key] = build_sma_signal_vector(
                features,
                direction,
                spread_pips=spread_pips,
            )
    sma_vector_sec = time.monotonic() - sma_prepare_started
    sma_model_started = time.monotonic()
    if sma_filter_model is None:
        sma_forecasts = {
            key: {
                "ready": False,
                "reason": "sma_filter_model_disabled",
                "feature_count": len(vector),
            }
            for key, vector in sma_vectors.items()
        }
    else:
        sma_forecasts = sma_filter_model.predict_many(sma_vectors)
    sma_model_sec = time.monotonic() - sma_model_started

    def prepare_combination_features() -> None:
        nonlocal combination_prepared
        if combination_prepared:
            return
        for pair, pair_features in feature_cache.items():
            pair_features["strategy_vote_features"] = dict(strategy_votes.get(pair) or {})
            quote = prices.get(pair)
            depth = (price_metadata or {}).get(pair) or {}
            if quote is not None:
                pair_features["live_spread_pips"] = (
                    max(0.0, safe_float(quote.ask) - safe_float(quote.bid))
                    / max(1e-12, safe_float(pair_features.get("pip"), 0.0001))
                )
            for name in (
                "bid_top_liquidity",
                "ask_top_liquidity",
                "bid_total_liquidity",
                "ask_total_liquidity",
                "depth_imbalance",
                "bid_levels",
                "ask_levels",
            ):
                pair_features[name] = safe_float(depth.get(name))
            vector = build_signal_vector(pair_features)
            combination_vectors[pair] = vector
            pair_features["signal_combination_forecast"] = (
                {"ready": False, "reason": "combination_model_disabled"}
                if combination_model is None
                else combination_model.predict(vector)
            )
        combination_prepared = True

    lane_evaluation_started = time.monotonic()
    partial_shadow_publish = {
        "available": False,
        "received": 0,
        "published": 0,
    }
    partial_shadow_publish_sec = 0.0
    partial_shadow_publish_lane_count = 0
    partial_shadow_publish_threshold = partial_shadow_lane_threshold(len(lanes))
    horizon_schedule = tuple(horizons)
    entry_microstructure_cache = {
        instrument: {
            name: safe_float(features.get(name))
            for name in (
                *MICROSTRUCTURE_SCALAR_KEYS,
                "live_spread_pips",
                "depth_total_liquidity",
                "depth_log_total_liquidity",
                "depth_top_imbalance",
                "microprice_offset_pips",
                "quote_receive_age_sec",
                "order_book_available",
                "position_book_available",
                "volume_ratio_12",
                "volume_ratio_30",
                "current_volume",
            )
        }
        for instrument, features in feature_cache.items()
    }
    feature_contract_versions = {
        instrument: feature_contract_version(features)
        for instrument, features in feature_cache.items()
    }
    lane_model_versions = {lane.lane_id: lane_model_version(lane) for lane in lanes}
    lane_stream_refresh_at = time.monotonic()
    # Force a new read-only venue snapshot at the lane-consumption boundary;
    # feature serialization and SMA preparation may already have consumed a
    # material fraction of the strict quote TTL.
    lane_snapshot_refresh_at = -math.inf
    for lane_index, lane in enumerate(lanes, start=1):
        # A full 208-lane pass can exceed the quote TTL.  Fold in the live
        # stream snapshot during the pass; unchanged/quiet quotes still age
        # out through outcome_quote_rejection_reason instead of being treated
        # as current.
        lane_now = time.monotonic()
        if lane_now - lane_stream_refresh_at >= 5.0:
            refresh_cycle_prices(
                price_provider,
                provider_name="stream",
                stage="lane_evaluation",
            )
            lane_stream_refresh_at = time.monotonic()
        if (
            snapshot_price_provider is not None
            and lane_now - lane_snapshot_refresh_at
            >= LANE_QUOTE_REFRESH_INTERVAL_SEC
        ):
            refresh_cycle_prices(
                snapshot_price_provider,
                provider_name="rest_snapshot",
                stage="lane_evaluation",
                audit=True,
            )
            lane_snapshot_refresh_at = time.monotonic()
        if lane.family in META_FAMILIES:
            prepare_combination_features()
        reasons: Counter[str] = Counter()
        raw_setups = 0
        accepted = 0
        missed = 0
        for instrument in instruments:
            features = feature_cache.get(instrument)
            if features is None:
                reasons["feature_unavailable"] += 1
                continue
            quote = prices.get(instrument)
            if quote is None:
                reasons["missing_price"] += 1
                continue
            quote_rejection = outcome_quote_rejection_reason(quote)
            if quote_rejection:
                reasons[quote_rejection] += 1
                continue
            setup = evaluate_family(lane, features)
            if setup.direction is None:
                reasons[setup.reason] += 1
                continue
            if lane.family not in META_FAMILIES:
                vote_sign = 1.0 if setup.direction == "buy" else -1.0
                strategy_votes[instrument][f"raw_{lane.family}_vote"] += vote_sign / len(PROFILES)
                strategy_votes[instrument][f"raw_{lane.family}_activity"] += 1.0 / len(PROFILES)
            raw_setups += 1
            decision_candle_time = str(setup.signal.get("decision_candle_time") or features["candle_time"])
            decision_key = (lane.lane_id, instrument, decision_candle_time)
            if decision_key in seen:
                reasons["duplicate_candle_setup"] += 1
                continue
            seen.add(decision_key)
            blockers, gate_metrics = economic_gates(lane, features, quote.bid, quote.ask, setup.signal)
            kind = "miss" if blockers else "signal"
            event = "shadow_miss" if blockers else "shadow_signal"
            miss_class = classify_miss(lane, gate_metrics) if blockers else ""
            if blockers:
                missed += 1
                reasons.update(blockers)
            else:
                accepted += 1
                if lane.family not in META_FAMILIES:
                    vote_sign = 1.0 if setup.direction == "buy" else -1.0
                    strategy_votes[instrument][f"accepted_{lane.family}_vote"] += vote_sign / len(PROFILES)
                    strategy_votes[instrument][f"accepted_{lane.family}_activity"] += 1.0 / len(PROFILES)
            model_version = lane_model_versions[lane.lane_id]
            feature_version = feature_contract_versions[instrument]
            event_id = canonical_forecast_id(
                lane,
                instrument,
                decision_candle_time,
                model_version,
                feature_version,
            )
            sma_key = (instrument, str(setup.direction))
            sma_vector = sma_vectors.get(sma_key, {})
            sma_forecast = sma_forecasts.get(
                sma_key,
                {
                    "ready": False,
                    "reason": "sma_features_unavailable",
                    "feature_count": 0,
                },
            )
            if sma_filter_store is not None and sma_vector:
                sma_filter_store.register_candidate(
                    event_id=event_id,
                    instrument=instrument,
                    origin_time=decision_candle_time,
                    direction=str(setup.direction),
                    lane_id=lane.lane_id,
                    family=lane.family,
                    profile=lane.profile,
                    kind=kind,
                    entry_time=str(quote.time),
                    entry_spread_pips=gate_metrics["spread_pips"],
                    features=sma_vector,
                )
                sma_registered += 1
            volatility_regime = classify_volatility_regime(
                gate_metrics.get("atr_pips"),
                gate_metrics.get("spread_pips"),
            )
            log_line(
                log_path,
                event,
                _count_only=bool(
                    blockers
                    and miss_class == "hard_reject"
                    and not sampled_detail(
                        event_id,
                        0,
                        hard_reject_log_sample_rate,
                    )
                ),
                id=event_id,
                lane_id=lane.lane_id,
                family=lane.family,
                profile=lane.profile,
                instrument=instrument,
                direction=setup.direction,
                blocked_reason=blockers[0] if blockers else "",
                blocked_reasons=blockers,
                miss_class=miss_class,
                entry_bid=quote.bid,
                entry_ask=quote.ask,
                entry_time=quote.time,
                entry_quote_source=str(getattr(quote, "source", "") or ""),
                signal_candle_time=decision_candle_time,
                account_eligible=bool(lane.parameters.get("account_eligible", True)),
                volatility_regime=volatility_regime,
                gates=gate_metrics,
                signal=setup.signal,
                sma_filter=sma_forecast,
            )
            lane_account_eligible = bool(
                lane.parameters.get("account_eligible", True)
            )
            depth = (price_metadata or {}).get(instrument) or {}
            preconsensus_class = (
                "accepted" if not blockers else miss_class or "hard_reject"
            )
            matrix_input_weight = {
                "accepted": 1.0,
                "near_threshold": 0.25,
                "hard_reject": 0.10,
            }.get(preconsensus_class, 0.10)
            accepted_candidates.append(
                {
                        "id": event_id,
                        "lane_id": lane.lane_id,
                        "family": lane.family,
                        "profile": lane.profile,
                        "model_id": lane.lane_id,
                        "input_timeframe": setup.signal.get("input_timeframe") or "multi",
                        "training_timeframe": "live_oanda",
                        "signal_role": "structural",
                        "instrument": instrument,
                        "direction": setup.direction,
                        "bid": quote.bid,
                        "ask": quote.ask,
                        "entry_time": quote.time,
                        "entry_quote_source": str(
                            getattr(quote, "source", "") or ""
                        ),
                        "pip": features["pip"],
                        "spread_pips": gate_metrics["spread_pips"],
                        "signal_to_spread": gate_metrics["signal_to_spread"],
                        "signal_strength_pips": abs(safe_float(setup.signal.get("strength_pips"))),
                        "reward_to_spread": gate_metrics["reward_to_spread"],
                        "atr_pips": gate_metrics["atr_pips"],
                        "bid_top_liquidity": safe_float(depth.get("bid_top_liquidity")),
                        "ask_top_liquidity": safe_float(depth.get("ask_top_liquidity")),
                        "bid_total_liquidity": safe_float(depth.get("bid_total_liquidity")),
                        "ask_total_liquidity": safe_float(depth.get("ask_total_liquidity")),
                        "depth_imbalance": safe_float(depth.get("depth_imbalance")),
                        "bid_levels": int(safe_float(depth.get("bid_levels"))),
                        "ask_levels": int(safe_float(depth.get("ask_levels"))),
                        "volatility_regime": volatility_regime,
                        "probability_up": (setup.signal.get("pattern_forecast") or {}).get("probability_up"),
                        "predicted_signed_pips": setup.signal.get("expected_signed_move_pips"),
                        "signal_reference_horizon_sec": int(
                            safe_float(
                                setup.signal.get("reference_horizon_sec"),
                                60
                                if setup.signal.get("expected_signed_move_pips") is not None
                                else 300,
                            )
                        ),
                        "trend_score": setup.signal.get("trend_score"),
                        "trend_conviction_fraction": setup.signal.get("conviction_fraction"),
                        "research_risk_weight": setup.signal.get("research_risk_weight"),
                        "stop_loss_pips": lane.parameters["stop_loss_pips"],
                        "take_profit_r": lane.parameters["take_profit_r"],
                        "account_eligible": lane_account_eligible,
                        "research_only": bool(blockers) or not lane_account_eligible,
                        "research_blocked_reason": (
                            "lane_shadow_only" if not lane_account_eligible else ""
                        ),
                        "preconsensus_class": preconsensus_class,
                        "preconsensus_blockers": list(blockers),
                        "matrix_input_weight": matrix_input_weight,
                        "sma_filter": sma_forecast,
                    }
                )
            reference_horizon = int(
                safe_float(
                    setup.signal.get("reference_horizon_sec"),
                    60
                    if setup.signal.get("expected_signed_move_pips") is not None
                    else 300,
                )
            )
            lane_horizons = horizon_schedule
            if shadow_outcome_horizon_mode == "declared" and horizon_schedule:
                lane_horizons = [
                    min(horizon_schedule, key=lambda value: abs(float(value) - reference_horizon))
                ]
            pending_item = (
                {
                    "id": event_id,
                    "lane_id": lane.lane_id,
                    "family": lane.family,
                    "profile": lane.profile,
                    "model_id": lane.lane_id,
                    "model_version": model_version,
                    "feature_version": feature_version,
                    "data_cutoff_utc": decision_candle_time,
                    "input_timeframe": setup.signal.get("input_timeframe") or "multi",
                    "training_timeframe": "live_oanda",
                    "kind": kind,
                    "instrument": instrument,
                    "direction": setup.direction,
                    "blocked_reason": blockers[0] if blockers else "",
                    "miss_class": miss_class,
                    "entry_bid": quote.bid,
                    "entry_ask": quote.ask,
                    "entry_mid": (quote.bid + quote.ask) / 2.0,
                    "entry_time": quote.time,
                    "entry_quote_source": str(
                        getattr(quote, "source", "") or ""
                    ),
                    "pip": features["pip"],
                    "entry_spread_pips": gate_metrics["spread_pips"],
                    "volatility_regime": volatility_regime,
                    "pattern_prediction": setup.signal.get("pattern_forecast"),
                    "probability_up": (
                        (setup.signal.get("pattern_forecast") or {}).get("probability_up")
                        if isinstance(setup.signal.get("pattern_forecast"), dict)
                        else setup.signal.get("probability_up")
                    ),
                    "predicted_magnitude_pips": abs(
                        safe_float(
                            setup.signal.get("expected_signed_move_pips"),
                            setup.signal.get("strength_pips"),
                        )
                    ),
                    "sma_filter": sma_forecast,
                    "stop_loss_pips": lane.parameters["stop_loss_pips"],
                    "take_profit_r": lane.parameters["take_profit_r"],
                    "max_favorable_pips": 0.0,
                    "max_adverse_pips": 0.0,
                    "first_positive_sec": None,
                    "path_samples": 0,
                    "positive_path_samples": 0,
                    "microstructure_at_entry": entry_microstructure_cache.get(
                        instrument,
                        {},
                    ),
                    "created_monotonic": time.monotonic(),
                    "remaining_horizons": lane_horizons,
                    "signal_reference_horizon_sec": reference_horizon,
                    "forecast_diagnostics": {
                        "signal": setup.signal,
                        "economic_gates": gate_metrics,
                        "blockers": list(blockers),
                        "account_eligible": lane_account_eligible,
                    },
                    **(
                        {"favorable_hits": {}, "adverse_hits": {}}
                        if kind == "signal"
                        else {}
                    ),
                }
            )
            track_outcome = should_track_shadow_outcome(
                event_id,
                kind,
                miss_class,
                hard_reject_outcome_sample_rate,
                near_threshold_outcome_sample_rate,
            )
            if outcome_store is not None:
                outcome_store.observe_forecast(
                    pending_item,
                    horizons=lane_horizons,
                    track_outcome=track_outcome,
                )
            if track_outcome:
                pending.append(pending_item)
        log_line(
            log_path,
            "lane_evaluation_summary",
            lane_id=lane.lane_id,
            family=lane.family,
            profile=lane.profile,
            evaluated=len(instruments),
            raw_setups=raw_setups,
            accepted=accepted,
            missed=missed,
            reason_counts=dict(reasons),
        )
        if (
            partial_shadow_publish_lane_count == 0
            and lane_index >= partial_shadow_publish_threshold
        ):
            partial_shadow_publish_started = time.monotonic()
            partial_shadow_publish = (
                {
                    "available": False,
                    "received": len(accepted_candidates),
                    "published": 0,
                }
                if executor is None
                else executor.publish_partial_cycle_shadow(
                    accepted_candidates
                )
            )
            partial_shadow_publish_sec = (
                time.monotonic() - partial_shadow_publish_started
            )
            partial_shadow_publish_lane_count = lane_index
            log_line(
                log_path,
                "partial_cycle_shadow_publish",
                **partial_shadow_publish,
                elapsed_sec=round(partial_shadow_publish_sec, 3),
                lane_count=partial_shadow_publish_lane_count,
                total_lane_count=len(lanes),
                execution_authorized=False,
            )
    lane_evaluation_sec = time.monotonic() - lane_evaluation_started
    timeframe_matrix_started = time.monotonic()
    timeframe_matrix_forecasts = 0
    if timeframe_matrix is not None:
        usable_prices = {
            instrument: quote
            for instrument, quote in prices.items()
            if not outcome_quote_rejection_reason(quote)
        }
        timeframe_matrix_forecasts = timeframe_matrix.emit(
            feature_cache,
            usable_prices,
            pip_sizes,
            horizons,
            pending,
            lambda event, **fields: log_line(log_path, event, **fields),
            candidate_sink=accepted_candidates,
        )
    timeframe_matrix_sec = time.monotonic() - timeframe_matrix_started
    combination_started = time.monotonic()
    prepare_combination_features()
    registered = 0
    if combination_store is not None and combination_pending is not None and combination_seen is not None:
        for instrument, vector in combination_vectors.items():
            features = feature_cache[instrument]
            quote = prices.get(instrument)
            origin_time = str(features.get("candle_time") or "")
            decision_key = (instrument, origin_time)
            if (
                not vector
                or outcome_quote_rejection_reason(quote)
                or decision_key in combination_seen
            ):
                continue
            combination_seen.add(decision_key)
            snapshot_id = f"{cycle_id}-{instrument}"
            combination_store.register_snapshot(snapshot_id, instrument, origin_time, vector)
            combination_pending.append(
                {
                    "snapshot_id": snapshot_id,
                    "instrument": instrument,
                    "origin_time": origin_time,
                    "entry_bid": quote.bid,
                    "entry_ask": quote.ask,
                    "entry_mid": (quote.bid + quote.ask) / 2.0,
                    "pip": features["pip"],
                    "created_monotonic": time.monotonic(),
                    "remaining_horizons": list(horizons),
                }
            )
            registered += 1
        combination_store.flush()
    combination_sec = time.monotonic() - combination_started
    sma_flush_started = time.monotonic()
    if sma_filter_store is not None:
        sma_filter_store.flush(force=True)
    sma_flush_sec = time.monotonic() - sma_flush_started
    execution_started = time.monotonic()
    if executor is not None:
        executor.maybe_execute(accepted_candidates)
    execution_sec = time.monotonic() - execution_started
    supervised_features = next(
        (features for features in feature_cache.values() if features.get("supervised_ready")),
        {},
    )
    log_line(
        log_path,
        "lab_evaluation_cycle",
        feature_instruments=len(feature_cache),
        feature_errors=dict(feature_errors),
        quote_refresh={
            "policy": "newest_broker_timestamp_overlay",
            "lane_interval_sec": LANE_QUOTE_REFRESH_INTERVAL_SEC,
            "freshness_threshold_sec": OUTCOME_QUOTE_MAX_AGE_SEC,
            "timestamps_preserved": True,
            "tradeability_preserved": True,
            "totals": dict(sorted(quote_refresh_totals.items())),
        },
        latency={
            "primary_features_sec": round(
                primary_feature_elapsed_sec,
                3,
            ),
            "ma_forecasts_ready_sec": round(ma_grid_elapsed_sec, 3),
            "ma_request_sec": round(ma_grid_request_sec, 3),
            "ma_inference_sec": round(ma_grid_inference_sec, 3),
            "ma_publish_sec": round(ma_grid_publish_sec, 3),
            "ma_postprocess_sec": round(ma_grid_postprocess_sec, 3),
            "timeframe_features_ready_sec": round(
                timeframe_feature_elapsed_sec,
                3,
            ),
            "snapshot_write_sec": round(snapshot_write_sec, 3),
            "lane_evaluation_sec": round(lane_evaluation_sec, 3),
            "partial_shadow_publish_sec": round(
                partial_shadow_publish_sec,
                3,
            ),
            "timeframe_matrix_sec": round(timeframe_matrix_sec, 3),
            "combination_sec": round(combination_sec, 3),
            "sma_vector_sec": round(sma_vector_sec, 3),
            "sma_model_sec": round(sma_model_sec, 3),
            "sma_flush_sec": round(sma_flush_sec, 3),
            "execution_sec": round(execution_sec, 3),
            "cycle_total_sec": round(
                time.monotonic() - cycle_started_monotonic,
                3,
            ),
        },
        pending=len(pending),
        pattern_count={
            **pattern_cycle,
            **({} if pattern_forecaster is None else pattern_forecaster.metadata()),
        },
        supervised_model={
            "ready": bool(supervised_features),
            "rows": int(safe_float(supervised_features.get("supervised_model_rows"))),
            "validation_rows": int(safe_float(supervised_features.get("supervised_validation_rows"))),
            "validation_hit_rate": round(safe_float(supervised_features.get("supervised_validation_hit_rate")), 4),
            "validation_avg_norm": round(safe_float(supervised_features.get("supervised_validation_avg_norm")), 4),
            "top_instrument": str(supervised_features.get("supervised_top_instrument") or ""),
            "top_expected_net_pips": round(safe_float(supervised_features.get("supervised_top_expected_net_pips")), 4),
        },
        combination_audit={
            "registered_snapshots": registered,
            "pending": 0 if combination_pending is None else len(combination_pending),
            "features_per_snapshot": 0 if not combination_vectors else max(len(vector) for vector in combination_vectors.values()),
        },
        sma_filter={
            "registered_candidates": sma_registered,
            "market_direction_vectors": len(sma_vectors),
            "features_per_vector": (
                0 if not sma_vectors else max(len(vector) for vector in sma_vectors.values())
            ),
        },
        ma_feature_grid={
            **(
                {}
                if ma_grid_runtime is None
                else ma_grid_runtime.metadata()
            ),
            "new_forecasts": ma_grid_forecasts,
            "batch": ma_grid_batch_stats,
            "timeframe_counts": dict(ma_grid_timeframes),
            "quote_rejections": dict(ma_grid_quote_rejections),
            "feed_publish": ma_grid_publish,
            "top": ma_grid_top[:10],
        },
        partial_cycle_shadow_publish=partial_shadow_publish,
        timeframe_horizon_matrix={
            "new_forecasts": timeframe_matrix_forecasts,
            **(
                {}
                if timeframe_matrix is None
                else timeframe_matrix.summary(horizons)
            ),
        },
        live_model_feature_snapshot={
            "path": "" if live_feature_snapshot_path is None else str(live_feature_snapshot_path.resolve()),
            "instruments": live_feature_snapshot_instruments,
        },
    )


def process_pending(
    prices: dict[str, Any],
    pending: list[dict[str, Any]] | deque[dict[str, Any]],
    log_path: Path,
    performance: LanePerformance | None = None,
    exit_fit: StrategyExitFit | None = None,
    promotion_store: LanePromotionStore | None = None,
    outcome_store: ShadowOutcomeStore | None = None,
    outcome_detail_sample_rate: float = 1.0,
    max_outcomes: int = 0,
    sma_filter_store: SmaSignalFilterStore | None = None,
    max_scan_items: int = 0,
) -> int:
    now = time.monotonic()
    emitted = 0
    emitted_pips = 0.0
    positive_outcomes = 0
    kind_counts: Counter[str] = Counter()
    horizon_counts: Counter[int] = Counter()
    censored = 0
    censored_reasons: Counter[str] = Counter()
    censored_horizons: Counter[int] = Counter()
    rotating_queue = isinstance(pending, deque)
    if rotating_queue:
        scan_count = len(pending)
        if max_scan_items > 0:
            scan_count = min(scan_count, int(max_scan_items))
        scan_items = (pending.popleft() for _ in range(scan_count))
    else:
        scan_items = iter(list(pending))
    for item in scan_items:
        remaining: list[float] = []
        age = now - safe_float(item["created_monotonic"])
        matured = [horizon for horizon in item["remaining_horizons"] if age >= horizon]
        fit_horizons = (
            tuple(getattr(exit_fit, "horizons_sec", (exit_fit.horizon_sec,)))
            if exit_fit is not None
            else (300,)
        )
        path_limit = float(max(fit_horizons))
        track_path = (
            str(item.get("kind") or "") == "signal"
            and bool(item.get("track_exit_path", True))
            and (age <= path_limit or any(horizon <= path_limit for horizon in matured))
            and (
                now - safe_float(item.get("last_path_sample_monotonic"), -1e18)
                >= 1.0
            )
        )
        if not matured and not track_path:
            if rotating_queue:
                pending.append(item)
            continue
        quote = prices.get(str(item["instrument"]))
        rejection_reason = outcome_quote_rejection_reason(quote)
        if rejection_reason:
            censored_now = [
                horizon
                for horizon in item["remaining_horizons"]
                if age >= horizon + OUTCOME_QUOTE_GRACE_SEC
            ]
            if not censored_now:
                if rotating_queue:
                    pending.append(item)
                continue
            censored_set = set(censored_now)
            item["remaining_horizons"] = [
                horizon
                for horizon in item["remaining_horizons"]
                if horizon not in censored_set
            ]
            censored += len(censored_now)
            censored_reasons[rejection_reason] += len(censored_now)
            for horizon in censored_now:
                censored_horizons[int(horizon)] += 1
                if outcome_store is not None and hasattr(
                    outcome_store, "observe_integrity_event"
                ):
                    outcome_store.observe_integrity_event(
                        event_id=str(item.get("id") or ""),
                        horizon_sec=horizon,
                        event_type="outcome_censored",
                        reason=rejection_reason,
                        details={
                            "instrument": str(item.get("instrument") or ""),
                            "entry_time": str(item.get("entry_time") or ""),
                            "age_sec": round(age, 3),
                            "recovered_from_ledger": bool(
                                item.get("recovered_from_ledger")
                            ),
                        },
                    )
            if rotating_queue:
                if item["remaining_horizons"]:
                    pending.append(item)
            elif not item["remaining_horizons"]:
                pending.remove(item)
            continue
        current_pips = theoretical_pips(
            str(item["direction"]),
            safe_float(item["pip"]),
            safe_float(item["entry_bid"]),
            safe_float(item["entry_ask"]),
            quote.bid,
            quote.ask,
        )
        if str(item.get("kind") or "") == "signal":
            favorable_hits = item.setdefault("favorable_hits", {})
            adverse_hits = item.setdefault("adverse_hits", {})
        else:
            favorable_hits = {}
            adverse_hits = {}
        if track_path:
            item["last_path_sample_monotonic"] = now
            item["path_samples"] = int(safe_float(item.get("path_samples"))) + 1
            if current_pips > 0.0:
                item["positive_path_samples"] = int(
                    safe_float(item.get("positive_path_samples"))
                ) + 1
                if item.get("first_positive_sec") is None:
                    item["first_positive_sec"] = round(age, 3)
            item["max_favorable_pips"] = max(safe_float(item.get("max_favorable_pips")), current_pips)
            item["max_adverse_pips"] = max(safe_float(item.get("max_adverse_pips")), -current_pips)
            for level in PATH_LEVELS:
                key = level_key(level)
                if current_pips >= level and key not in favorable_hits:
                    favorable_hits[key] = round(age, 3)
                if current_pips <= -level and key not in adverse_hits:
                    adverse_hits[key] = round(age, 3)
        for horizon in item["remaining_horizons"]:
            if max_outcomes > 0 and emitted >= max_outcomes:
                remaining.append(horizon)
                continue
            if age < horizon:
                remaining.append(horizon)
                continue
            pips = current_pips
            if performance is not None:
                performance.observe(
                    str(item["lane_id"]),
                    horizon,
                    pips,
                    str(item["kind"]),
                )
            pattern_diagnostics: dict[str, Any] = {}
            prediction = item.get("pattern_prediction")
            if isinstance(prediction, dict) and math.isclose(
                horizon,
                safe_float(prediction.get("target_horizon_sec")),
                abs_tol=1e-6,
            ):
                entry_mid = safe_float(
                    item.get("entry_mid"),
                    (safe_float(item["entry_bid"]) + safe_float(item["entry_ask"])) / 2.0,
                )
                exit_mid = (quote.bid + quote.ask) / 2.0
                actual_signed = (exit_mid - entry_mid) / max(safe_float(item["pip"]), 1e-12)
                expected_signed = safe_float(prediction.get("expected_signed_move_pips"))
                expected_abs = safe_float(prediction.get("expected_abs_move_pips"))
                baseline_abs = max(1e-9, safe_float(prediction.get("baseline_abs_move_pips")))
                probability_up = safe_float(prediction.get("probability_up"), 0.5)
                actual_up = 1.0 if actual_signed > 0.0 else 0.0
                predicted_direction = str(prediction.get("predicted_direction") or "")
                direction_correct = (
                    (predicted_direction == "buy" and actual_signed > 0.0)
                    or (predicted_direction == "sell" and actual_signed < 0.0)
                )
                pattern_diagnostics = {
                    "pattern_prediction": prediction,
                    "actual_signed_move_pips": round(actual_signed, 4),
                    "actual_abs_move_pips": round(abs(actual_signed), 4),
                    "actual_movement_coefficient": round(abs(actual_signed) / baseline_abs, 6),
                    "direction_correct": direction_correct,
                    "signed_move_error_pips": round(actual_signed - expected_signed, 4),
                    "absolute_move_error_pips": round(abs(actual_signed) - expected_abs, 4),
                    "probability_brier_score": round((probability_up - actual_up) ** 2, 6),
                }
            outcome_payload = {
                "id": item["id"],
                "lane_id": item["lane_id"],
                "family": item["family"],
                "profile": item["profile"],
                "model_id": item.get("model_id") or "",
                "input_timeframe": item.get("input_timeframe") or "",
                "training_timeframe": item.get("training_timeframe") or "",
                "model_version": item.get("model_version") or "unknown",
                "feature_version": item.get("feature_version") or "unknown",
                "data_cutoff_utc": item.get("data_cutoff_utc") or item.get("entry_time") or "",
                "recovered_from_ledger": bool(item.get("recovered_from_ledger")),
                "kind": item["kind"],
                "instrument": item["instrument"],
                "direction": item["direction"],
                "blocked_reason": item["blocked_reason"],
                "miss_class": item.get("miss_class") or "",
                "volatility_regime": item.get("volatility_regime") or "unknown",
                "horizon_sec": horizon,
                "theoretical_pips": round(pips, 3),
                "outcome_delay_sec": round(max(0.0, age - horizon), 3),
                "entry_time": item["entry_time"],
                "exit_time": quote.time,
                "entry_bid": safe_float(item.get("entry_bid")),
                "entry_ask": safe_float(item.get("entry_ask")),
                "exit_bid": quote.bid,
                "exit_ask": quote.ask,
                "pip": safe_float(item.get("pip")),
                "entry_spread_pips": round(
                    max(
                        0.0,
                        safe_float(
                            item.get("entry_spread_pips"),
                            (
                                safe_float(item.get("entry_ask"))
                                - safe_float(item.get("entry_bid"))
                            )
                            / max(safe_float(item.get("pip")), 1e-12),
                        ),
                    ),
                    4,
                ),
                "max_favorable_pips": round(
                    safe_float(item.get("max_favorable_pips")),
                    3,
                ),
                "max_adverse_pips": round(
                    safe_float(item.get("max_adverse_pips")),
                    3,
                ),
                "first_positive_sec": item.get("first_positive_sec"),
                "path_samples": int(safe_float(item.get("path_samples"))),
                "positive_path_samples": int(
                    safe_float(item.get("positive_path_samples"))
                ),
                "configured_stop_pips": safe_float(item.get("stop_loss_pips")),
                "configured_target_r": safe_float(item.get("take_profit_r")),
                "configured_stop_hit_sec": adverse_hits.get(
                    level_key(safe_float(item.get("stop_loss_pips")))
                ),
                "configured_target_hit_sec": favorable_hits.get(
                    level_key(
                        safe_float(item.get("stop_loss_pips"))
                        * safe_float(item.get("take_profit_r"))
                    )
                ),
                "favorable_hits": dict(favorable_hits),
                "adverse_hits": dict(adverse_hits),
                "microstructure_at_entry": dict(
                    item.get("microstructure_at_entry") or {}
                ),
                **pattern_diagnostics,
            }
            forecast_curve = item.get("forecast_curve") or {}
            timeframe_prediction = forecast_curve.get(str(int(horizon)))
            if isinstance(timeframe_prediction, dict):
                outcome_payload["timeframe_prediction"] = timeframe_prediction
            if outcome_store is not None:
                outcome_store.observe(outcome_payload)
            if (
                outcome_store is None
                or bool(pattern_diagnostics)
                or sampled_detail(
                    str(item["id"]),
                    int(horizon),
                    outcome_detail_sample_rate,
                )
            ):
                log_line(log_path, "shadow_outcome", **outcome_payload)
            if exit_fit is not None and bool(item.get("track_exit_path", True)):
                exit_fit.observe(
                    event_id=item["id"],
                    horizon_sec=horizon,
                    lane_id=item["lane_id"],
                    family=item["family"],
                    profile=item["profile"],
                    model_id=item.get("model_id") or item["lane_id"],
                    input_timeframe=item.get("input_timeframe") or "",
                    kind=item["kind"],
                    instrument=item["instrument"],
                    direction=item["direction"],
                    entry_time=item["entry_time"],
                    exit_time=quote.time,
                    endpoint_pips=pips,
                    entry_spread_pips=outcome_payload["entry_spread_pips"],
                    max_favorable_pips=item.get("max_favorable_pips"),
                    max_adverse_pips=item.get("max_adverse_pips"),
                    first_positive_sec=item.get("first_positive_sec"),
                    path_samples=item.get("path_samples"),
                    positive_path_samples=item.get("positive_path_samples"),
                    favorable_hits=favorable_hits,
                    adverse_hits=adverse_hits,
                    volatility_regime=item.get("volatility_regime") or "",
                )
            if promotion_store is not None:
                promotion_store.observe(
                    event_id=item["id"],
                    horizon_sec=horizon,
                    lane_id=item["lane_id"],
                    family=item["family"],
                    profile=item["profile"],
                    kind=item["kind"],
                    instrument=item["instrument"],
                    direction=item["direction"],
                    entry_time=item["entry_time"],
                    endpoint_pips=pips,
                )
            if sma_filter_store is not None and item.get("sma_filter") is not None:
                sma_filter_store.observe(
                    str(item["id"]),
                    horizon,
                    str(quote.time),
                    pips,
                    safe_float(item.get("max_favorable_pips")),
                    safe_float(item.get("max_adverse_pips")),
                )
            emitted += 1
            emitted_pips += pips
            positive_outcomes += int(pips > 0.0)
            kind_counts[str(item.get("kind") or "")] += 1
            horizon_counts[int(horizon)] += 1
        item["remaining_horizons"] = remaining
        if rotating_queue:
            if remaining:
                pending.append(item)
        elif not remaining:
            pending.remove(item)
    if exit_fit is not None and emitted:
        exit_fit.flush()
    if promotion_store is not None and emitted:
        promotion_store.flush()
    if outcome_store is not None and emitted:
        log_line(
            log_path,
            "shadow_outcome_batch",
            count=emitted,
            positive_count=positive_outcomes,
            average_pips=round(emitted_pips / emitted, 4),
            kind_counts=dict(kind_counts),
            horizon_counts={str(key): value for key, value in sorted(horizon_counts.items())},
            detail_sample_rate=round(max(0.0, min(1.0, outcome_detail_sample_rate)), 6),
            storage=outcome_store.summary(),
        )
    if sma_filter_store is not None and emitted:
        sma_filter_store.flush(force=True)
    if censored:
        log_line(
            log_path,
            "shadow_outcome_censored_batch",
            count=censored,
            reason_counts=dict(censored_reasons),
            horizon_counts={
                str(key): value for key, value in sorted(censored_horizons.items())
            },
            policy="target quote must be tradeable and no more than 30 seconds old",
        )
    return emitted


def process_combination_pending(
    prices: dict[str, Any],
    pending: list[dict[str, Any]],
    store: SignalCombinationStore,
    log_path: Path | None = None,
) -> int:
    now = time.monotonic()
    emitted = 0
    censored = 0
    censored_reasons: Counter[str] = Counter()
    censored_horizons: Counter[int] = Counter()
    for item in list(pending):
        quote = prices.get(str(item["instrument"]))
        age = now - safe_float(item["created_monotonic"])
        rejection_reason = outcome_quote_rejection_reason(quote)
        if rejection_reason:
            censored_now = [
                horizon
                for horizon in item["remaining_horizons"]
                if age >= horizon + OUTCOME_QUOTE_GRACE_SEC
            ]
            if not censored_now:
                continue
            censored_set = set(censored_now)
            item["remaining_horizons"] = [
                horizon
                for horizon in item["remaining_horizons"]
                if horizon not in censored_set
            ]
            censored += len(censored_now)
            censored_reasons[rejection_reason] += len(censored_now)
            for horizon in censored_now:
                censored_horizons[int(horizon)] += 1
            if not item["remaining_horizons"]:
                pending.remove(item)
            continue
        remaining: list[float] = []
        for horizon in item["remaining_horizons"]:
            if age < horizon:
                remaining.append(horizon)
                continue
            pip = max(1e-12, safe_float(item["pip"]))
            exit_mid = (quote.bid + quote.ask) / 2.0
            signed_move = (exit_mid - safe_float(item["entry_mid"])) / pip
            long_net = theoretical_pips(
                "buy", pip, safe_float(item["entry_bid"]), safe_float(item["entry_ask"]), quote.bid, quote.ask
            )
            short_net = theoretical_pips(
                "sell", pip, safe_float(item["entry_bid"]), safe_float(item["entry_ask"]), quote.bid, quote.ask
            )
            store.observe(
                str(item["snapshot_id"]),
                horizon,
                str(quote.time),
                signed_move,
                long_net,
                short_net,
            )
            emitted += 1
        item["remaining_horizons"] = remaining
        if not remaining:
            pending.remove(item)
    if emitted:
        store.flush()
    if censored and log_path is not None:
        log_line(
            log_path,
            "combination_outcome_censored_batch",
            count=censored,
            reason_counts=dict(censored_reasons),
            horizon_counts={
                str(key): value for key, value in sorted(censored_horizons.items())
            },
            policy="target quote must be tradeable and no more than 30 seconds old",
        )
    return emitted


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--creds", type=Path, default=DEFAULT_CREDS)
    parser.add_argument("--account-key", default="OANDA_ACCOUNT_ID_DUM4")
    parser.add_argument("--account-id", default="")
    parser.add_argument("--duration-sec", type=int, default=28800)
    parser.add_argument("--scan-pause-sec", type=float, default=0.25)
    parser.add_argument("--candle-close-delay-sec", type=float, default=2.0)
    parser.add_argument("--candle-workers", type=int, default=16)
    parser.add_argument("--slow-candle-refresh-sec", type=float, default=300.0)
    parser.add_argument("--daily-candle-refresh-sec", type=float, default=3600.0)
    parser.add_argument("--stream-start-timeout-sec", type=float, default=10.0)
    parser.add_argument("--stream-stale-sec", type=float, default=12.0)
    parser.add_argument("--rest-fallback-sec", type=float, default=1.0)
    parser.add_argument(
        "--use-price-stream",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--outcome-horizons",
        type=parse_horizons,
        default=[float(value) for value in FULL_HORIZONS_SEC],
    )
    parser.add_argument(
        "--shadow-outcome-horizon-mode",
        choices=("all", "declared"),
        default="all",
        help="Mature every configured horizon or only the signal's declared horizon.",
    )
    parser.add_argument(
        "--timeframe-matrix-outcome-mode",
        choices=("full", "native"),
        default="full",
        help="Mature every matrix horizon or only the horizon nearest each input timeframe.",
    )
    parser.add_argument("--instruments", default="")
    parser.add_argument("--exclude-regex", default="")
    parser.add_argument(
        "--families",
        default=",".join((*DEFAULT_FAMILIES, *sorted(RESEARCH_SHADOW_FAMILIES))),
    )
    parser.add_argument("--profiles", default=",".join((*PROFILES, "shadow")))
    parser.add_argument("--lane-config", type=Path, default=None)
    parser.add_argument("--run-label", default="expanded-156-lane-multihorizon-v7")
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument(
        "--heartbeat-state",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "strategy_lab_heartbeat_v1.json",
    )
    parser.add_argument(
        "--live-model-feature-snapshot",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "live_model_feature_snapshot_v1.json",
    )
    parser.add_argument(
        "--order-book-feature-state",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "oanda_order_position_book_latest_v1.json",
    )
    parser.add_argument("--order-book-feature-max-age-sec", type=float, default=1800.0)
    parser.add_argument("--pattern-history-cache", type=Path, default=DEFAULT_PATTERN_HISTORY_CACHE)
    parser.add_argument("--pattern-live-state", type=Path, default=DEFAULT_PATTERN_LIVE_STATE)
    parser.add_argument("--pattern-bootstrap-candles", type=int, default=5000)
    parser.add_argument("--pattern-historical-effective-cap", type=float, default=1000.0)
    parser.add_argument("--pattern-live-weight", type=float, default=4.0)
    parser.add_argument("--pattern-prior-count", type=float, default=20.0)
    parser.add_argument(
        "--combination-database",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "signal_combination_audit_v1.sqlite",
    )
    parser.add_argument(
        "--combination-state",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "signal_combination_audit_v1.json",
    )
    parser.add_argument(
        "--combination-deep-state",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "signal_combination_deep_v1.json",
    )
    parser.add_argument(
        "--combination-historical-state",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "signal_combination_historical_v1.json",
    )
    parser.add_argument(
        "--enable-sma-filter",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--sma-filter-database",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "sma_signal_filter_v1.sqlite",
    )
    parser.add_argument("--sma-filter-batch-size", type=int, default=16384)
    parser.add_argument(
        "--sma-filter-artifact",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "sma_signal_filter_v1.joblib",
    )
    parser.add_argument(
        "--enable-ma-feature-grid",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--ma-feature-grid-artifact",
        type=Path,
        default=(
            DEFAULT_LOG_DIR.parent
            / "models"
            / "ma_feature_grid"
            / "ma_feature_grid_latest.joblib"
        ),
    )
    parser.add_argument(
        "--exit-fit-database",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "strategy_exit_fit_v1.sqlite",
    )
    parser.add_argument(
        "--exit-fit-state",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "strategy_exit_fit_v1.json",
    )
    parser.add_argument(
        "--shadow-outcome-database",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "strategy_shadow_outcomes_v1.sqlite",
    )
    parser.add_argument("--shadow-outcome-batch-size", type=int, default=4096)
    parser.add_argument("--shadow-outcome-flush-sec", type=float, default=1.0)
    parser.add_argument("--shadow-outcome-log-sample-rate", type=float, default=0.01)
    parser.add_argument("--hard-reject-log-sample-rate", type=float, default=0.05)
    parser.add_argument("--hard-reject-outcome-sample-rate", type=float, default=0.05)
    parser.add_argument("--near-threshold-outcome-sample-rate", type=float, default=0.50)
    parser.add_argument("--outcome-max-per-cycle", type=int, default=256)
    parser.add_argument("--outcome-max-scan-per-cycle", type=int, default=16384)
    parser.add_argument(
        "--timeframe-matrix-calibration-state",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "timeframe_matrix_calibration_v1.json",
    )
    parser.add_argument(
        "--exit-fit-auto-fit",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--exit-fit-record-outcomes",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--promotion-database",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "lane_promotion_v1.sqlite",
    )
    parser.add_argument(
        "--promotion-state",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "lane_promotion_v1.json",
    )
    parser.add_argument(
        "--enable-second-forecasts",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--second-forecast-model",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "second_ridge_models_v1.json",
    )
    parser.add_argument(
        "--second-forecast-database",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "second_forecast_live_v1.sqlite",
    )
    parser.add_argument(
        "--second-forecast-state",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "second_forecast_live_v1.json",
    )
    parser.add_argument("--second-forecast-cadence-sec", type=int, default=1)
    parser.add_argument("--second-forecast-sample-sec", type=int, default=5)
    parser.add_argument("--second-forecast-signal-interval-sec", type=int, default=5)
    parser.add_argument(
        "--enable-signal-engine",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--execute-top-signals", action="store_true")
    parser.add_argument("--execution-ranking-log", type=Path, default=None)
    parser.add_argument("--execution-ranking-horizon-sec", type=int, default=300)
    parser.add_argument(
        "--execution-horizons",
        type=parse_horizons,
        default=[float(value) for value in FULL_HORIZONS_SEC],
    )
    parser.add_argument("--execution-min-samples", type=int, default=30)
    parser.add_argument("--execution-min-average-pips", type=float, default=0.10)
    parser.add_argument("--execution-min-median-pips", type=float, default=0.0)
    parser.add_argument("--execution-min-win-rate", type=float, default=52.0)
    parser.add_argument("--execution-min-lower-confidence-pips", type=float, default=0.0)
    parser.add_argument("--execution-min-independent-blocks", type=int, default=12)
    parser.add_argument("--execution-min-holdout-blocks", type=int, default=4)
    parser.add_argument("--execution-min-pairs", type=int, default=3)
    parser.add_argument("--execution-min-sessions", type=int, default=2)
    parser.add_argument("--execution-min-segment-samples", type=int, default=10)
    parser.add_argument("--promotion-refresh-sec", type=float, default=300.0)
    parser.add_argument("--execution-top-lanes", type=int, default=8)
    parser.add_argument("--execution-snapshot-top-signals", type=int, default=32)
    parser.add_argument("--execution-units", type=int, default=100)
    parser.add_argument("--execution-max-units", type=int, default=5000)
    parser.add_argument("--execution-cooldown-sec", type=float, default=30.0)
    parser.add_argument(
        "--execution-reentry-cooldown-sec",
        type=float,
        default=3600.0,
        help="Cooldown after an exit before re-entering the same pair and direction.",
    )
    parser.add_argument(
        "--execution-instrument-reentry-cooldown-sec",
        type=float,
        default=1800.0,
        help="Cooldown after an exit before trading either direction of that pair.",
    )
    parser.add_argument(
        "--execution-jpy-factor-cooldown-sec",
        type=float,
        default=3600.0,
        help="Cooldown shared by all pairs expressing the same JPY direction.",
    )
    parser.add_argument(
        "--execution-max-open-jpy-factor-positions",
        type=int,
        default=1,
        help="Maximum simultaneous positions expressing one JPY direction factor.",
    )
    parser.add_argument(
        "--execution-reentry-history-sec",
        type=float,
        default=172800.0,
    )
    parser.add_argument(
        "--execution-intrahour-cost-gate",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--execution-intrahour-cost-gate-max-horizon-sec",
        type=int,
        default=3600,
    )
    parser.add_argument(
        "--execution-intrahour-max-spread-pips",
        type=float,
        default=2.0,
    )
    parser.add_argument(
        "--execution-intrahour-min-liquidity-quality",
        type=float,
        default=0.70,
    )
    parser.add_argument(
        "--execution-intrahour-min-confidence",
        type=float,
        default=0.56,
    )
    parser.add_argument(
        "--execution-intrahour-min-gross-to-spread",
        type=float,
        default=2.5,
    )
    parser.add_argument(
        "--execution-intrahour-min-after-cost-pips",
        type=float,
        default=1.0,
    )
    parser.add_argument(
        "--execution-multihour-cost-gate",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--execution-multihour-max-spread-pips",
        type=float,
        default=3.0,
    )
    parser.add_argument(
        "--execution-multihour-min-liquidity-quality",
        type=float,
        default=0.65,
    )
    parser.add_argument(
        "--execution-multihour-min-confidence",
        type=float,
        default=0.55,
    )
    parser.add_argument(
        "--execution-multihour-min-gross-to-spread",
        type=float,
        default=2.0,
    )
    parser.add_argument(
        "--execution-multihour-min-after-cost-pips",
        type=float,
        default=1.0,
    )
    parser.add_argument(
        "--execution-horizon-scaled-protection",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--execution-max-hold-sec", type=float, default=300.0)
    parser.add_argument("--execution-max-quote-age-sec", type=float, default=8.0)
    parser.add_argument("--execution-max-slippage-pips", type=float, default=0.5)
    parser.add_argument("--execution-max-loss-account", type=float, default=2.0)
    parser.add_argument("--execution-recent-veto-min-samples", type=int, default=5)
    parser.add_argument("--execution-recent-veto-max-win-rate", type=float, default=40.0)
    parser.add_argument("--execution-min-historical-reliability", type=float, default=0.15)
    parser.add_argument(
        "--execution-allow-unvalidated-signals",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument(
        "--execution-paper-consensus",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    parser.add_argument("--execution-paper-consensus-min-families", type=int, default=3)
    parser.add_argument(
        "--execution-paper-consensus-min-aligned-weight-pct",
        type=float,
        default=65.0,
    )
    parser.add_argument(
        "--execution-paper-consensus-min-confidence",
        type=float,
        default=0.545,
    )
    parser.add_argument(
        "--execution-paper-consensus-min-net-pips",
        type=float,
        default=0.15,
    )
    parser.add_argument(
        "--execution-paper-consensus-min-gross-to-spread",
        type=float,
        default=1.15,
    )
    parser.add_argument(
        "--execution-paper-consensus-max-horizon-sec",
        type=int,
        default=3600,
    )
    parser.add_argument("--execution-manage-interval-sec", type=float, default=5.0)
    parser.add_argument(
        "--execution-signal-feed-database",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "practice_007_signal_feed_v1.sqlite",
    )
    parser.add_argument(
        "--execution-policy-state",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "practice_007_execution_policy_v1.json",
    )
    parser.add_argument("--execution-feed-source", default="strategy_lab")
    parser.add_argument(
        "--execution-feed-consumer-only",
        action="store_true",
        help=(
            "Read the initialized consolidated feed without registering "
            "inventory or publishing local candidates."
        ),
    )
    parser.add_argument("--execution-signal-feed-ttl-sec", type=float, default=8.0)
    parser.add_argument("--execution-signal-feed-limit", type=int, default=50_000)
    parser.add_argument(
        "--execution-signal-feed-cache-refresh-sec",
        type=float,
        default=5.0,
        help="Background refresh interval for a consumer-only feed cache.",
    )
    parser.add_argument(
        "--execution-signal-feed-cache-ttl-sec",
        type=float,
        default=None,
        help=(
            "Fail-closed cache TTL for consumer-only execution; defaults "
            "to --execution-signal-feed-ttl-sec."
        ),
    )
    parser.add_argument(
        "--execution-signal-snapshot",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "practice_007_signal_snapshot_v1.json",
    )
    parser.add_argument(
        "--execution-signal-snapshot-sec",
        type=float,
        default=None,
        help="Minimum interval between derived signal snapshot rewrites.",
    )
    parser.add_argument(
        "--research-market-quote-snapshot",
        type=Path,
        default=None,
        help=(
            "Optional strategy-lab-owned research quote snapshot. The canonical "
            "Practice-007 quote snapshot is owned exclusively by the fast executor "
            "and must never be used here."
        ),
    )
    parser.add_argument(
        "--research-market-quote-snapshot-sec",
        type=float,
        default=5.0,
    )
    parser.add_argument("--execution-min-signal-confidence", type=float, default=0.56)
    parser.add_argument("--execution-min-signal-expected-net-pips", type=float, default=1.0)
    parser.add_argument(
        "--execution-prediction-quality",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--execution-prediction-quality-min-samples", type=int, default=30)
    parser.add_argument("--execution-prediction-quality-max-weight", type=float, default=0.75)
    parser.add_argument(
        "--execution-prediction-quality-negative-veto",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--execution-max-open-positions", type=int, default=4)
    parser.add_argument("--execution-max-currency-direction-positions", type=int, default=3)
    parser.add_argument(
        "--execution-max-currency-direction-margin-pct",
        type=float,
        default=45.0,
    )
    parser.add_argument("--execution-target-margin-used-pct", type=float, default=55.0)
    parser.add_argument("--execution-high-confidence-margin-used-pct", type=float, default=68.0)
    parser.add_argument("--execution-hard-margin-used-pct", type=float, default=75.0)
    parser.add_argument("--execution-min-trade-margin-pct", type=float, default=12.0)
    parser.add_argument("--execution-max-trade-margin-pct", type=float, default=24.0)
    parser.add_argument("--execution-min-risk-pct", type=float, default=0.8)
    parser.add_argument("--execution-max-risk-pct", type=float, default=2.5)
    parser.add_argument("--execution-min-trailing-pips", type=float, default=2.5)
    parser.add_argument("--execution-trailing-spread-multiple", type=float, default=2.5)
    parser.add_argument("--execution-trailing-stop-r", type=float, default=0.55)
    parser.add_argument("--execution-trailing-activation-r", type=float, default=0.9)
    parser.add_argument(
        "--execution-trailing-activation-spread-multiple",
        type=float,
        default=0.5,
    )
    parser.add_argument("--execution-profit-lock-trigger-pips", type=float, default=2.0)
    parser.add_argument("--execution-profit-lock-floor-pips", type=float, default=0.2)
    parser.add_argument("--execution-profit-lock-spread-multiple", type=float, default=1.5)
    parser.add_argument("--execution-profit-lock-step-pips", type=float, default=0.25)
    parser.add_argument("--execution-profit-hold-multiplier", type=float, default=2.0)
    parser.add_argument(
        "--execution-second-forecast-state",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "second_forecast_hot_v1.json",
    )
    parser.add_argument("--execution-second-curve-max-age-sec", type=float, default=30.0)
    parser.add_argument("--execution-second-curve-max-horizon-sec", type=int, default=300)
    parser.add_argument("--execution-second-curve-min-points", type=int, default=3)
    parser.add_argument("--execution-second-curve-oppose-probability", type=float, default=0.47)
    parser.add_argument("--execution-second-curve-min-net-pips", type=float, default=0.05)
    parser.add_argument("--execution-second-curve-min-aligned-fraction", type=float, default=0.5)
    parser.add_argument("--execution-second-curve-min-trade-age-sec", type=float, default=10.0)
    parser.add_argument("--execution-second-curve-profit-exit-pips", type=float, default=0.3)
    parser.add_argument(
        "--execution-second-curve-entry-veto",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--execution-second-curve-profit-exit",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--execution-dynamic-sizing",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--execution-open-ended-profit",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--execution-manage-trades",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--execution-lock-path",
        type=Path,
        default=DEFAULT_LOG_DIR.parent / "state" / "practice_007_order.lock",
    )
    parser.add_argument(
        "--execution-use-fitted-exits",
        action=argparse.BooleanOptionalAction,
        default=False,
    )
    args = parser.parse_args(argv)
    if args.execute_top_signals and not args.enable_signal_engine:
        raise SystemExit("practice execution requires the signal engine")
    if (
        args.duration_sec <= 0
        or args.scan_pause_sec <= 0.0
        or args.candle_workers <= 0
        or args.stream_start_timeout_sec <= 0.0
        or args.stream_stale_sec <= 0.0
        or args.rest_fallback_sec <= 0.0
        or args.order_book_feature_max_age_sec <= 0.0
        or args.pattern_bootstrap_candles < 120
        or args.pattern_bootstrap_candles > 5000
        or args.pattern_historical_effective_cap <= 0.0
        or args.pattern_live_weight < 0.0
        or args.pattern_prior_count < 0.0
        or args.execution_ranking_horizon_sec <= 0
        or args.execution_min_samples <= 0
        or args.execution_min_independent_blocks <= 0
        or args.execution_min_holdout_blocks <= 0
        or args.execution_min_pairs <= 0
        or args.execution_min_sessions <= 0
        or args.execution_min_segment_samples <= 0
        or args.promotion_refresh_sec <= 0.0
        or args.second_forecast_cadence_sec <= 0
        or args.second_forecast_sample_sec <= 0
        or args.second_forecast_signal_interval_sec <= 0
        or args.execution_min_win_rate < 0.0
        or args.execution_min_win_rate > 100.0
        or args.execution_top_lanes <= 0
        or args.execution_snapshot_top_signals <= 0
        or args.execution_units <= 0
        or args.execution_max_units <= 0
        or args.execution_cooldown_sec < 0.0
        or args.execution_reentry_cooldown_sec < 0.0
        or args.execution_instrument_reentry_cooldown_sec < 0.0
        or args.execution_jpy_factor_cooldown_sec < 0.0
        or args.execution_max_open_jpy_factor_positions <= 0
        or args.execution_reentry_history_sec <= 0.0
        or args.execution_intrahour_cost_gate_max_horizon_sec <= 0
        or args.execution_intrahour_max_spread_pips <= 0.0
        or not 0.0 <= args.execution_intrahour_min_liquidity_quality <= 1.0
        or not 0.5 <= args.execution_intrahour_min_confidence < 1.0
        or args.execution_intrahour_min_gross_to_spread < 1.0
        or args.execution_intrahour_min_after_cost_pips < 0.0
        or args.execution_multihour_max_spread_pips <= 0.0
        or not 0.0 <= args.execution_multihour_min_liquidity_quality <= 1.0
        or not 0.5 <= args.execution_multihour_min_confidence < 1.0
        or args.execution_multihour_min_gross_to_spread < 1.0
        or args.execution_multihour_min_after_cost_pips < 0.0
        or args.execution_max_hold_sec <= 0.0
        or args.execution_max_quote_age_sec <= 0.0
        or args.execution_max_slippage_pips <= 0.0
        or args.execution_max_loss_account <= 0.0
        or args.execution_recent_veto_min_samples <= 0
        or not 0.0 <= args.execution_recent_veto_max_win_rate <= 100.0
        or not 0.0 <= args.execution_min_historical_reliability <= 1.0
        or args.execution_paper_consensus_min_families < 2
        or not 50.0 <= args.execution_paper_consensus_min_aligned_weight_pct <= 100.0
        or not 0.5 <= args.execution_paper_consensus_min_confidence < 1.0
        or args.execution_paper_consensus_min_net_pips < 0.0
        or args.execution_paper_consensus_min_gross_to_spread < 1.0
        or args.execution_paper_consensus_max_horizon_sec <= 0
        or args.execution_manage_interval_sec <= 0.0
        or args.execution_signal_feed_ttl_sec <= 0.0
        or args.execution_signal_feed_limit <= 0
        or not 0.5 <= args.execution_min_signal_confidence < 1.0
        or args.execution_prediction_quality_min_samples <= 0
        or not 0.0 <= args.execution_prediction_quality_max_weight <= 1.0
        or args.execution_second_curve_min_net_pips < 0.0
        or not 0.0 <= args.execution_second_curve_min_aligned_fraction <= 1.0
        or args.execution_max_open_positions <= 0
        or args.execution_max_currency_direction_positions <= 0
        or not 0.0 < args.execution_max_currency_direction_margin_pct <= 100.0
        or not 0.0 < args.execution_target_margin_used_pct < args.execution_hard_margin_used_pct < 100.0
        or not args.execution_target_margin_used_pct <= args.execution_high_confidence_margin_used_pct <= args.execution_hard_margin_used_pct
        or not 0.0 < args.execution_min_trade_margin_pct <= args.execution_max_trade_margin_pct
        or not 0.0 < args.execution_min_risk_pct <= args.execution_max_risk_pct
        or args.execution_min_trailing_pips <= 0.0
        or args.execution_trailing_spread_multiple <= 0.0
        or args.execution_trailing_stop_r <= 0.0
        or args.execution_trailing_activation_r <= 0.0
        or args.execution_trailing_activation_spread_multiple < 0.0
        or args.execution_profit_lock_trigger_pips <= 0.0
        or args.execution_profit_lock_floor_pips < 0.0
        or args.execution_profit_lock_spread_multiple <= 0.0
        or args.execution_profit_lock_step_pips <= 0.0
        or args.execution_profit_hold_multiplier < 1.0
        or args.shadow_outcome_batch_size <= 0
        or args.sma_filter_batch_size <= 0
        or args.shadow_outcome_flush_sec <= 0.0
        or args.outcome_max_per_cycle <= 0
        or args.outcome_max_scan_per_cycle <= 0
        or not 0.0 <= args.shadow_outcome_log_sample_rate <= 1.0
        or not 0.0 <= args.hard_reject_log_sample_rate <= 1.0
        or not 0.0 <= args.hard_reject_outcome_sample_rate <= 1.0
        or not 0.0 <= args.near_threshold_outcome_sample_rate <= 1.0
    ):
        raise SystemExit("duration, data timing, and candle worker arguments must be positive")
    outcome_horizons = {int(value) for value in args.outcome_horizons}
    execution_horizons = {int(value) for value in args.execution_horizons}
    if not execution_horizons.issubset(outcome_horizons):
        raise SystemExit("execution horizons must be included in outcome horizons")
    args.execution_horizons = sorted(execution_horizons)
    return args


def filter_lanes(lanes: list[LaneSpec], families: str, profiles: str) -> list[LaneSpec]:
    family_filter = {item.strip() for item in families.split(",") if item.strip()}
    profile_filter = {item.strip() for item in profiles.split(",") if item.strip()}
    unknown_families = family_filter.difference(FAMILIES)
    unknown_profiles = profile_filter.difference((*PROFILES, "shadow"))
    if unknown_families:
        raise SystemExit(f"Unknown families: {', '.join(sorted(unknown_families))}")
    if unknown_profiles:
        raise SystemExit(f"Unknown profiles: {', '.join(sorted(unknown_profiles))}")
    return [lane for lane in lanes if lane.family in family_filter and lane.profile in profile_filter]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    lanes = filter_lanes(load_lanes(args.lane_config), args.families, args.profiles)
    if not lanes:
        raise SystemExit("No strategy lanes selected.")
    args.log_dir.mkdir(parents=True, exist_ok=True)
    token, account_id = read_credentials(args.creds, args.account_key, args.account_id)
    instruments = selected_instruments(args, token, account_id)
    if not instruments:
        raise SystemExit("No tradeable FX instruments selected.")
    client = MarketDataClient(token)
    pip_sizes = account_pip_sizes(client, account_id)
    missing_pip_sizes = [instrument for instrument in instruments if instrument not in pip_sizes]
    pattern_forecaster: PatternCountForecaster | None = None
    if any(lane.family == "pattern_count_forecast" for lane in lanes):
        pattern_forecaster = PatternCountForecaster(
            args.pattern_history_cache,
            args.pattern_live_state,
            historical_effective_cap=args.pattern_historical_effective_cap,
            live_weight=args.pattern_live_weight,
            prior_count=args.pattern_prior_count,
        )
    timeframe_matrix = TimeframeHorizonMatrix(
        calibration_state_path=args.timeframe_matrix_calibration_state,
        minimum_timeframe_sec=60,
        outcome_horizon_mode=args.timeframe_matrix_outcome_mode,
    )
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    selector = re.sub(r"[^a-z0-9]+", "_", str(args.account_key or "account").lower()).strip("_")
    log_path = args.log_dir / f"practice_strategy_lab_{selector}_{stamp}.jsonl"
    heartbeat = WorkerHeartbeat(
        args.heartbeat_state,
        worker="oanda_practice_shadow_strategy_lab",
        role="strategy_lab",
    ).start()
    heartbeat.update(
        phase="initializing",
        account_suffix=account_id[-4:],
        instrument_count=len(instruments),
        lane_count=len(lanes),
        run_label=args.run_label,
    )
    performance = LanePerformance(args.execution_ranking_horizon_sec)
    exit_fit = StrategyExitFit(
        args.exit_fit_database,
        args.exit_fit_state,
        horizon_sec=args.execution_ranking_horizon_sec,
        horizons_sec=args.execution_horizons,
        fit_enabled=args.exit_fit_auto_fit,
        record_enabled=args.exit_fit_record_outcomes,
    )
    outcome_store = ShadowOutcomeStore(
        args.shadow_outcome_database,
        batch_size=args.shadow_outcome_batch_size,
        flush_sec=args.shadow_outcome_flush_sec,
    )
    if args.exit_fit_record_outcomes and not args.exit_fit_state.is_file():
        exit_fit.fit()
    promotion_store = LanePromotionStore(args.promotion_database)
    promotion = LanePromotionModel(
        args.promotion_database,
        args.promotion_state,
        args.execution_horizons,
        thresholds=PromotionThresholds(
            min_samples=args.execution_min_samples,
            min_average_pips=args.execution_min_average_pips,
            min_median_pips=args.execution_min_median_pips,
            min_win_rate=args.execution_min_win_rate,
            min_lower_confidence_pips=args.execution_min_lower_confidence_pips,
            min_independent_blocks=args.execution_min_independent_blocks,
            min_holdout_blocks=args.execution_min_holdout_blocks,
            min_pairs=args.execution_min_pairs,
            min_sessions=args.execution_min_sessions,
            min_segment_samples=args.execution_min_segment_samples,
        ),
        refresh_sec=args.promotion_refresh_sec,
        fit_enabled=False,
    )
    promotion.load_state()
    combination_store = SignalCombinationStore(args.combination_database)
    combination_model = SignalCombinationModel(
        args.combination_state,
        [args.combination_deep_state, args.combination_historical_state],
        maximum_state_age_sec=86400.0,
    )
    sma_filter_store = (
        SmaSignalFilterStore(
            args.sma_filter_database,
            batch_size=args.sma_filter_batch_size,
        )
        if args.enable_sma_filter
        else None
    )
    sma_filter_model = (
        SmaSignalFilterModel(args.sma_filter_artifact)
        if args.enable_sma_filter
        else None
    )
    ma_grid_runtime = (
        MaFeatureGridRuntime(args.ma_feature_grid_artifact, minimum_timeframe_sec=60)
        if args.enable_ma_feature_grid
        else None
    )
    second_runtime: SecondForecastRuntime | None = None
    second_runtime_error = ""
    if args.enable_second_forecasts:
        try:
            second_runtime = SecondForecastRuntime(
                args.second_forecast_model,
                args.second_forecast_database,
                args.second_forecast_state,
                cadence_sec=args.second_forecast_cadence_sec,
                sample_sec=args.second_forecast_sample_sec,
                signal_interval_sec=args.second_forecast_signal_interval_sec,
                execution_horizons=set(args.execution_horizons),
            )
        except Exception as exc:
            second_runtime_error = f"{type(exc).__name__}: {exc}"
    ranking_rows = 0
    if args.execute_top_signals and args.execution_ranking_log is not None:
        ranking_rows = performance.load(args.execution_ranking_log)
    log_line(
        log_path,
        "lab_start",
        run_label=args.run_label,
        mode=(
            "shadow_plus_practice_execution"
            if args.execute_top_signals
            else "shadow_plus_signal_engine"
            if args.enable_signal_engine
            else "shadow_only"
        ),
        account_suffix=account_id[-4:],
        instrument_count=len(instruments),
        instruments=instruments,
        lane_count=len(lanes),
        lanes=[asdict(lane) for lane in lanes],
        outcome_horizons=args.outcome_horizons,
        pricing_interval_sec=args.scan_pause_sec,
        pricing_source="oanda_stream" if args.use_price_stream else "rest_poll",
        pricing_stream_max_updates_per_instrument_sec=4 if args.use_price_stream else 0,
        pip_size_source="oanda_instrument_metadata",
        pip_size_count=len(pip_sizes),
        missing_pip_sizes=missing_pip_sizes,
        candle_refresh="next M1 close",
        pattern_count={} if pattern_forecaster is None else pattern_forecaster.metadata(),
        signal_combination={
            "database": str(args.combination_database.resolve()),
            "state": str(args.combination_state.resolve()),
            "method": "fuzzy 2-3 signal rules with chronological holdout",
        },
        sma_filter={
            "enabled": bool(args.enable_sma_filter),
            "database": str(args.sma_filter_database.resolve()),
            "artifact": str(args.sma_filter_artifact.resolve()),
            "store": (
                {}
                if sma_filter_store is None
                else sma_filter_store.summary()
            ),
        },
        ma_feature_grid={
            "enabled": bool(args.enable_ma_feature_grid),
            "artifact": str(args.ma_feature_grid_artifact.resolve()),
            **(
                {}
                if ma_grid_runtime is None
                else ma_grid_runtime.metadata()
            ),
            "policy": "shadow forecasts contribute to the unified matrix; account eligibility remains false",
        },
        second_forecast={
            "enabled": second_runtime is not None,
            "model": str(args.second_forecast_model.resolve()),
            "model_ready": bool(second_runtime and second_runtime.ready),
            "database": str(args.second_forecast_database.resolve()),
            "state": str(args.second_forecast_state.resolve()),
            "cadence_sec": args.second_forecast_cadence_sec,
            "sample_sec": args.second_forecast_sample_sec,
            "transaction_horizons_sec": [
                horizon for horizon in (15, 30, 60, 300) if horizon in set(args.execution_horizons)
            ],
            "error": second_runtime_error,
        },
        exit_fit={
            "database": str(args.exit_fit_database.resolve()),
            "state": str(args.exit_fit_state.resolve()),
            "apply_eligible_recommendations": bool(args.execution_use_fitted_exits),
        },
        shadow_outcomes={
            **outcome_store.summary(),
            "detail_log_sample_rate": args.shadow_outcome_log_sample_rate,
            "hard_reject_log_sample_rate": args.hard_reject_log_sample_rate,
        },
        timeframe_horizon_matrix=timeframe_matrix.summary(args.outcome_horizons),
        timeframe_matrix_calibration_state=str(
            args.timeframe_matrix_calibration_state.resolve()
        ),
        live_model_feature_snapshot=str(args.live_model_feature_snapshot.resolve()),
        order_position_book={
            "state": str(args.order_book_feature_state.resolve()),
            "max_age_sec": args.order_book_feature_max_age_sec,
            "usage": "shadow features plus combination rules; never direct account authorization",
        },
        heartbeat_state=str(args.heartbeat_state.resolve()),
        lane_promotion={
            "database": str(args.promotion_database.resolve()),
            "state": str(args.promotion_state.resolve()),
            "horizons_sec": list(args.execution_horizons),
            "model_type": "direct executable-net/no-trade with pair/session projection",
            "independent_blocks": True,
        },
        execution={
            "signal_engine_enabled": bool(args.enable_signal_engine),
            "enabled": args.execute_top_signals,
            "selection_mode": "individual_signal_confidence_stream",
            "signal_feed_database": str(args.execution_signal_feed_database.resolve()),
            "signal_feed_source": args.execution_feed_source,
            "signal_feed_limit": args.execution_signal_feed_limit,
            "min_signal_confidence": args.execution_min_signal_confidence,
            "min_signal_expected_net_pips": args.execution_min_signal_expected_net_pips,
            "dynamic_sizing": bool(args.execution_dynamic_sizing),
            "fallback_units": args.execution_units,
            "max_units": args.execution_max_units,
            "max_open_positions": args.execution_max_open_positions,
            "max_currency_direction_positions": args.execution_max_currency_direction_positions,
            "max_currency_direction_margin_pct": args.execution_max_currency_direction_margin_pct,
            "target_margin_used_pct": args.execution_target_margin_used_pct,
            "high_confidence_margin_used_pct": args.execution_high_confidence_margin_used_pct,
            "hard_margin_used_pct": args.execution_hard_margin_used_pct,
            "open_ended_profit": bool(args.execution_open_ended_profit),
            "profit_hold_multiplier": args.execution_profit_hold_multiplier,
            "ranking_horizon_sec": args.execution_ranking_horizon_sec,
            "ranking_horizons_sec": list(args.execution_horizons),
            "ranking_log": "" if args.execution_ranking_log is None else str(args.execution_ranking_log.resolve()),
            "ranking_rows": ranking_rows,
            "min_samples": args.execution_min_samples,
            "min_independent_blocks": args.execution_min_independent_blocks,
            "min_holdout_blocks": args.execution_min_holdout_blocks,
            "min_pairs": args.execution_min_pairs,
            "min_sessions": args.execution_min_sessions,
            "top_lanes": args.execution_top_lanes,
            "cooldown_sec": args.execution_cooldown_sec,
            "max_hold_sec": args.execution_max_hold_sec,
            "max_loss_account": args.execution_max_loss_account,
            "use_fitted_exits": bool(args.execution_use_fitted_exits),
        },
    )

    executor: PracticeExecutor | None = None
    if args.enable_signal_engine:
        executor = PracticeExecutor(
            client,
            account_id,
            log_path,
            performance,
            args,
            exit_fit,
            promotion,
        )
        executor.initialize()
        if executor.signal_feed is not None:
            contributor_rows = [
                {
                    "contributor_id": lane.lane_id,
                    "family": lane.family,
                    "source_kind": "strategy_lane",
                    "runtime_policy": "live_shadow_then_execution_filter",
                    "expected": True,
                    "account_eligible": bool(
                        lane.parameters.get("account_eligible", True)
                    ),
                    "metadata": {"profile": lane.profile},
                }
                for lane in lanes
            ]
            contributor_rows.extend(
                {
                    "contributor_id": f"timeframe_equation_matrix.{timeframe.lower()}",
                    "family": "timeframe_equation_matrix",
                    "source_kind": "continuous_equation",
                    "runtime_policy": "live_shadow_calibration_required",
                    "expected": True,
                    "account_eligible": False,
                    "metadata": {"input_timeframe": timeframe},
                }
                for timeframe in timeframe_matrix.summary(
                    args.outcome_horizons
                ).get("input_timeframes", [])
            )
            contributor_rows.append(
                {
                    "contributor_id": "second_ridge",
                    "family": "intrasecond_ridge",
                    "source_kind": "entry_exit_timing",
                    "runtime_policy": "timing_only_never_structural_entry",
                    "expected": bool(args.enable_second_forecasts),
                    "account_eligible": False,
                    "metadata": {"ready": bool(second_runtime and second_runtime.ready)},
                }
            )
            executor.signal_feed.register_contributors(contributor_rows)
            log_line(
                log_path,
                "signal_engine_inventory",
                execution_enabled=bool(args.execute_top_signals),
                **executor.signal_feed.coverage(),
            )

    stop_at = time.monotonic() + args.duration_sec
    next_refresh = 0.0
    next_tick_log = 0.0
    next_market_quote_snapshot = 0.0
    pending: deque[dict[str, Any]] = deque()
    seen: set[tuple[str, str, str]] = set()
    recovered_pending = outcome_store.recover_pending(
        args.outcome_horizons,
        horizon_mode=args.shadow_outcome_horizon_mode,
    )
    pending.extend(recovered_pending)
    for recovered_item in recovered_pending:
        seen_key = (
            str(recovered_item.get("lane_id") or ""),
            str(recovered_item.get("instrument") or ""),
            str(
                recovered_item.get("data_cutoff_utc")
                or recovered_item.get("entry_time")
                or ""
            ),
        )
        if all(seen_key):
            seen.add(seen_key)
    log_line(
        log_path,
        "canonical_forecast_recovery",
        recovered=len(recovered_pending),
        database=str(args.shadow_outcome_database.resolve()),
    )
    combination_pending: list[dict[str, Any]] = []
    combination_seen: set[tuple[str, str]] = set()
    prices: dict[str, Any] = {}
    candle_cache: dict[str, dict[str, list[dict[str, Any]]]] = {}
    timeframe_feature_memo: dict[
        tuple[str, str],
        tuple[str, dict[str, Any]],
    ] = {}
    pricing_ticks = 0
    rest_fallbacks = 0
    evaluation_cycles = 0
    outcome_count = 0
    combination_outcome_count = 0
    api_errors = 0
    last_api_error: dict[str, Any] | None = None
    stream: MultiPriceStream | None = None
    if args.use_price_stream:
        stream = MultiPriceStream(
            lambda: read_credentials(args.creds, args.account_key, args.account_id),
            instruments,
            lambda event, **fields: log_line(log_path, event, **fields),
            research_snapshot_path=args.research_market_quote_snapshot,
            research_snapshot_interval_sec=args.research_market_quote_snapshot_sec,
            pip_sizes=pip_sizes,
        )
        stream.start()
        stream_ready = stream.wait_ready(args.stream_start_timeout_sec)
        log_line(
            log_path,
            "lab_price_source_ready",
            source="oanda_stream" if stream_ready else "rest_fallback",
            ready=stream_ready,
            stream=stream.stats(),
        )
    if executor is not None:
        executor.set_price_snapshot_provider(
            lambda: execution_price_snapshot(
                stream,
                prices,
                snapshot_provider=lambda: client.pricing_snapshot(
                    account_id,
                    instruments,
                ),
            )
        )
    candle_pool = ThreadPoolExecutor(max_workers=args.candle_workers, thread_name_prefix="lab-candle")
    refresh_coordinator = ThreadPoolExecutor(max_workers=1, thread_name_prefix="lab-refresh")
    refresh_future: Any = None
    refresh_started = 0.0
    refresh_includes_m5 = True
    refresh_includes_daily = True
    last_slow_candle_refresh_monotonic = -math.inf
    last_daily_candle_refresh_monotonic = -math.inf
    refresh_m1_count = 120
    last_rest_fallback = 0.0
    try:
        while time.monotonic() < stop_at:
            try:
                stream_prices = stream.snapshot() if stream is not None else {}
                stream_healthy = bool(stream and stream.healthy(args.stream_stale_sec))
                stream_complete = len(stream_prices) >= max(1, math.ceil(len(instruments) * 0.95))
                if stream_healthy and stream_complete:
                    prices = stream_prices
                elif time.monotonic() - last_rest_fallback >= args.rest_fallback_sec:
                    rest_prices = client.pricing_snapshot(account_id, instruments)
                    prices = merge_price_maps(rest_prices, stream_prices)
                    last_rest_fallback = time.monotonic()
                    rest_fallbacks += 1
                else:
                    prices = merge_price_maps(prices, stream_prices)
                pricing_ticks += 1
                usable_prices = {
                    instrument: quote
                    for instrument, quote in prices.items()
                    if not outcome_quote_rejection_reason(quote)
                }
                if usable_prices:
                    timeframe_matrix.observe(usable_prices)
                if second_runtime is not None and usable_prices:
                    stream_metadata = stream.snapshot_metadata() if stream is not None else {}
                    second_runtime.evaluate(
                        usable_prices,
                        stream_metadata,
                        pip_sizes,
                        pending,
                        lambda event, **fields: log_line(log_path, event, **fields),
                        candidate_callback=None if executor is None else executor.maybe_execute,
                    )
                heartbeat.update(
                    phase="maturing_outcomes",
                    pending=len(pending),
                    combination_pending=len(combination_pending),
                    usable_priced_instruments=len(usable_prices),
                )
                outcome_count += process_pending(
                    prices,
                    pending,
                    log_path,
                    performance,
                    exit_fit,
                    promotion_store,
                    outcome_store,
                    args.shadow_outcome_log_sample_rate,
                    args.outcome_max_per_cycle,
                    sma_filter_store=sma_filter_store,
                    max_scan_items=args.outcome_max_scan_per_cycle,
                )
                combination_outcome_count += process_combination_pending(
                    prices,
                    combination_pending,
                    combination_store,
                    log_path,
                )
                if executor is not None and args.execute_top_signals:
                    executor.manage_open_trades(prices=usable_prices)

                if refresh_future is not None and refresh_future.done():
                    heartbeat.update(phase="building_features")
                    completed_future = refresh_future
                    refresh_future = None
                    next_refresh = next_refresh_epoch(args.candle_close_delay_sec)
                    candle_update, candle_errors = completed_future.result()
                    merge_candle_cache(candle_cache, candle_update)
                    if refresh_includes_m5 and any(
                        "M5" in granularities and "H1" in granularities
                        for granularities in candle_update.values()
                    ):
                        last_slow_candle_refresh_monotonic = time.monotonic()
                    if refresh_includes_daily and any(
                        "D" in granularities for granularities in candle_update.values()
                    ):
                        last_daily_candle_refresh_monotonic = time.monotonic()
                    entry_rest_prices = client.pricing_snapshot(account_id, instruments)
                    rest_fallbacks += 1
                    prices = merge_price_maps(entry_rest_prices, stream.snapshot() if stream is not None else {})
                    log_line(
                        log_path,
                        "lab_candle_refresh",
                        instruments=len(candle_update),
                        cached_instruments=len(candle_cache),
                        refreshed_m5=refresh_includes_m5,
                        refreshed_h1=refresh_includes_m5,
                        refreshed_d1=refresh_includes_daily,
                        m1_candle_count=refresh_m1_count,
                        candle_errors=dict(candle_errors),
                        elapsed_sec=round(time.monotonic() - refresh_started, 3),
                    )
                    pricing_metadata = (
                        stream.snapshot_metadata()
                        if stream is not None
                        else {}
                    )
                    book_metadata = load_order_position_book_features(
                        args.order_book_feature_state,
                        args.order_book_feature_max_age_sec,
                    )
                    evaluate_cycle(
                        lanes,
                        instruments,
                        candle_cache,
                        prices,
                        pip_sizes,
                        args.outcome_horizons,
                        log_path,
                        pending,
                        seen,
                        outcome_store=outcome_store,
                        executor=executor,
                        pattern_forecaster=pattern_forecaster,
                        timeframe_matrix=timeframe_matrix,
                        combination_model=combination_model,
                        combination_store=combination_store,
                        combination_pending=combination_pending,
                        combination_seen=combination_seen,
                        sma_filter_model=sma_filter_model,
                        sma_filter_store=sma_filter_store,
                        ma_grid_runtime=ma_grid_runtime,
                        price_metadata=merge_microstructure_metadata(
                            pricing_metadata,
                            book_metadata,
                        ),
                        live_feature_snapshot_path=args.live_model_feature_snapshot,
                        price_provider=(
                            None if stream is None else stream.snapshot
                        ),
                        snapshot_price_provider=(
                            lambda: merge_price_maps(
                                client.pricing_snapshot(account_id, instruments),
                                stream.snapshot() if stream is not None else {},
                            )
                        ),
                        timeframe_feature_memo=timeframe_feature_memo,
                        hard_reject_log_sample_rate=args.hard_reject_log_sample_rate,
                        hard_reject_outcome_sample_rate=(
                            args.hard_reject_outcome_sample_rate
                        ),
                        near_threshold_outcome_sample_rate=(
                            args.near_threshold_outcome_sample_rate
                        ),
                        shadow_outcome_horizon_mode=args.shadow_outcome_horizon_mode,
                    )
                    evaluation_cycles += 1
                    heartbeat.update(
                        phase="streaming",
                        evaluation_cycles=evaluation_cycles,
                        pending=len(pending),
                    )

                if refresh_future is None and time.time() >= next_refresh:
                    refresh_includes_m5 = slow_candle_refresh_due(
                        candle_cache,
                        instruments,
                        last_slow_candle_refresh_monotonic,
                        args.slow_candle_refresh_sec,
                    )
                    refresh_includes_daily = daily_candle_refresh_due(
                        candle_cache,
                        instruments,
                        last_daily_candle_refresh_monotonic,
                        args.daily_candle_refresh_sec,
                    )
                    refresh_started = time.monotonic()
                    refresh_m1_count = (
                        args.pattern_bootstrap_candles
                        if not candle_cache
                        and (
                            (pattern_forecaster is not None and pattern_forecaster.ready)
                            or (ma_grid_runtime is not None and ma_grid_runtime.ready)
                        )
                        else 120
                    )
                    refresh_future = refresh_coordinator.submit(
                        candle_snapshot,
                        client,
                        instruments,
                        candle_pool,
                        refresh_includes_m5,
                        refresh_m1_count,
                        refresh_includes_daily,
                    )
                    next_refresh = math.inf

                if (
                    args.research_market_quote_snapshot is not None
                    and time.monotonic() >= next_market_quote_snapshot
                    and prices
                ):
                    quote_rows = {}
                    for instrument, quote in prices.items():
                        if outcome_quote_rejection_reason(quote):
                            continue
                        quote_rows[instrument] = {
                            "bid": safe_float(quote.bid),
                            "ask": safe_float(quote.ask),
                            "time": str(quote.time or ""),
                            "pip": safe_float(
                                pip_sizes.get(
                                    instrument,
                                    0.01
                                    if instrument.endswith("_JPY")
                                    else 0.0001,
                                )
                            ),
                            "source": str(
                                getattr(quote, "source", "")
                                or "strategy_price_stream"
                            ),
                            "tradeable": bool(
                                getattr(quote, "tradeable", True)
                            ),
                        }
                    # The dedicated price-stream thread writes the same
                    # snapshot.  Never replace its valid quotes with an empty
                    # batch when every cached quote fails the freshness check.
                    if quote_rows:
                        atomic_json(
                            args.research_market_quote_snapshot,
                            {
                                "schema_version": 3,
                                "generated_utc": utc_now(),
                                "account_suffix": account_id[-4:],
                                "producer": "strategy_lab_price_stream",
                                "quote_count": len(quote_rows),
                                "quotes": quote_rows,
                                "research_only": True,
                            },
                        )
                    next_market_quote_snapshot = (
                        time.monotonic()
                        + max(
                            1.0,
                            args.research_market_quote_snapshot_sec,
                        )
                    )

                if time.monotonic() >= next_tick_log:
                    refresh_active = refresh_future is not None
                    event_counts = log_event_counts(log_path)
                    log_line(
                        log_path,
                        "lab_tick",
                        pricing_ticks=pricing_ticks,
                        evaluation_cycles=evaluation_cycles,
                        priced_instruments=len(prices),
                        usable_priced_instruments=len(usable_prices),
                        pending=len(pending),
                        outcomes=outcome_count,
                        combination_pending=len(combination_pending),
                        combination_outcomes=combination_outcome_count,
                        sma_filter=(
                            {}
                            if sma_filter_store is None
                            else sma_filter_store.summary()
                        ),
                        candle_refresh_in_progress=refresh_active,
                        candle_refresh_elapsed_sec=round(time.monotonic() - refresh_started, 2) if refresh_active else 0.0,
                        next_candle_refresh_in_sec=0.0 if refresh_active else round(max(0.0, next_refresh - time.time()), 2),
                        rest_fallbacks=rest_fallbacks,
                        price_stream={} if stream is None else stream.stats(),
                        second_forecast={} if second_runtime is None else second_runtime.summary(),
                    )
                    heartbeat.update(
                        phase="streaming",
                        pricing_ticks=pricing_ticks,
                        evaluation_cycles=evaluation_cycles,
                        priced_instruments=len(prices),
                        usable_priced_instruments=len(usable_prices),
                        pending=len(pending),
                        outcomes=outcome_count,
                        api_errors=api_errors,
                        last_api_error=last_api_error,
                        signals=event_counts.get("shadow_signal", 0),
                        near_misses=event_counts.get("shadow_miss:near_threshold", 0),
                        hard_rejects=event_counts.get("shadow_miss:hard_reject", 0),
                        run_label=args.run_label,
                    )
                    next_tick_log = time.monotonic() + 10.0
            except OandaApiError as exc:
                api_errors += 1
                last_api_error = {
                    "time": utc_now(),
                    "kind": type(exc).__name__,
                    "status": exc.status,
                    "message": str(exc)[:500],
                    "phase": heartbeat.phase,
                }
                log_line(
                    log_path,
                    "lab_api_error",
                    status=exc.status,
                    error=str(exc)[:500],
                    phase=heartbeat.phase,
                )
            except Exception as exc:
                api_errors += 1
                last_api_error = {
                    "time": utc_now(),
                    "kind": type(exc).__name__,
                    "status": None,
                    "message": str(exc)[:500],
                    "phase": heartbeat.phase,
                }
                log_line(
                    log_path,
                    "lab_cycle_error",
                    error_type=type(exc).__name__,
                    error=str(exc)[:500],
                    phase=heartbeat.phase,
                )
            time.sleep(min(args.scan_pause_sec, max(0.0, stop_at - time.monotonic())))
    finally:
        if prices:
            outcome_count += process_pending(
                prices,
                pending,
                log_path,
                performance,
                exit_fit,
                promotion_store,
                outcome_store,
                args.shadow_outcome_log_sample_rate,
                sma_filter_store=sma_filter_store,
            )
            combination_outcome_count += process_combination_pending(
                prices,
                combination_pending,
                combination_store,
            )
        refresh_coordinator.shutdown(wait=False, cancel_futures=True)
        candle_pool.shutdown(wait=False, cancel_futures=True)
        if stream is not None:
            stream.stop()
        if second_runtime is not None:
            second_runtime.close()
        combination_store.close()
        if sma_filter_store is not None:
            sma_filter_store.close()
        outcome_store.close()
        exit_fit.close()
        promotion_store.close()
        if executor is not None:
            executor.close()
        heartbeat.close()
        client.close()
    log_line(
        log_path,
        "lab_end",
        pricing_ticks=pricing_ticks,
        rest_fallbacks=rest_fallbacks,
        evaluation_cycles=evaluation_cycles,
        outcomes=outcome_count,
        combination_outcomes=combination_outcome_count,
        sma_filter=(
            {}
            if sma_filter_store is None
            else sma_filter_store.summary()
        ),
        practice_fills=0 if executor is None else executor.fills,
        execution_disabled_reason="" if executor is None else executor.disabled_reason,
        pattern_count={} if pattern_forecaster is None else pattern_forecaster.metadata(),
        timeframe_horizon_matrix=timeframe_matrix.summary(args.outcome_horizons),
        second_forecast={} if second_runtime is None else second_runtime.summary(),
        pending=len(pending),
        combination_pending=len(combination_pending),
        api_errors=api_errors,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
