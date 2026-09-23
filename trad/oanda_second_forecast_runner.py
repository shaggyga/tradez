#!/usr/bin/env python3
"""Run the one-second forecast and promotion-gated practice path independently."""

from __future__ import annotations

import argparse
import json
import math
import re
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from oanda_lane_promotion import LanePromotionModel, LanePromotionStore, PromotionThresholds
    from oanda_model_gap_live_signal_worker import LiveForecastLedger, atomic_json
    from oanda_practice_eurusd_micro_scalper import DEFAULT_CREDS, DEFAULT_LOG_DIR, OandaApiError
    from oanda_practice_pair_rotation_scalper import read_credentials, tradeable_currency_instruments
    from oanda_practice_shadow_strategy_lab import (
        LanePerformance,
        MarketDataClient,
        MultiPriceStream,
        PracticeExecutor,
        account_pip_sizes,
        log_line,
        merge_price_maps,
        process_pending,
    )
    from oanda_second_forecast import (
        DEFAULT_EXECUTION_HORIZONS_SEC,
        INPUT_TIMEFRAME,
        MODEL_FAMILY,
        TRAINING_TIMEFRAME,
        SecondForecastRuntime,
    )
    from oanda_shadow_outcome_store import ShadowOutcomeStore
    from oanda_signal_contribution_feed import SignalContributionFeed
    from oanda_strategy_exit_fit import StrategyExitFit
    from oanda_worker_heartbeat import WorkerHeartbeat
except ModuleNotFoundError:
    from trad.oanda_lane_promotion import LanePromotionModel, LanePromotionStore, PromotionThresholds
    from trad.oanda_model_gap_live_signal_worker import LiveForecastLedger, atomic_json
    from trad.oanda_practice_eurusd_micro_scalper import DEFAULT_CREDS, DEFAULT_LOG_DIR, OandaApiError
    from trad.oanda_practice_pair_rotation_scalper import read_credentials, tradeable_currency_instruments
    from trad.oanda_practice_shadow_strategy_lab import (
        LanePerformance,
        MarketDataClient,
        MultiPriceStream,
        PracticeExecutor,
        account_pip_sizes,
        log_line,
        merge_price_maps,
        process_pending,
    )
    from trad.oanda_second_forecast import (
        DEFAULT_EXECUTION_HORIZONS_SEC,
        INPUT_TIMEFRAME,
        MODEL_FAMILY,
        TRAINING_TIMEFRAME,
        SecondForecastRuntime,
    )
    from trad.oanda_shadow_outcome_store import ShadowOutcomeStore
    from trad.oanda_signal_contribution_feed import SignalContributionFeed
    from trad.oanda_strategy_exit_fit import StrategyExitFit
    from trad.oanda_worker_heartbeat import WorkerHeartbeat


def parse_horizons(value: str) -> list[int]:
    horizons = sorted({int(item) for item in re.split(r"[\s,]+", value.strip()) if item})
    if not horizons or any(horizon <= 0 for horizon in horizons):
        raise argparse.ArgumentTypeError("execution horizons must be positive")
    return horizons


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    state = DEFAULT_LOG_DIR.parent / "state"
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--creds", type=Path, default=DEFAULT_CREDS)
    parser.add_argument("--account-key", default="OANDA_ACCOUNT_ID_DUM4")
    parser.add_argument("--account-id", default="")
    parser.add_argument(
        "--runner-role",
        choices=("hot", "tracker", "microstructure"),
        default="hot",
    )
    parser.add_argument("--duration-sec", type=int, default=28800)
    parser.add_argument("--scan-pause-sec", type=float, default=0.10)
    parser.add_argument("--stream-start-timeout-sec", type=float, default=10.0)
    parser.add_argument("--stream-stale-sec", type=float, default=12.0)
    parser.add_argument("--rest-fallback-sec", type=float, default=1.0)
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument("--heartbeat-state", type=Path, default=None)
    parser.add_argument("--second-forecast-model", type=Path, default=state / "second_ridge_models_v1.json")
    parser.add_argument("--second-forecast-database", type=Path, default=state / "second_forecast_live_v1.sqlite")
    parser.add_argument("--second-forecast-state", type=Path, default=state / "second_forecast_live_v1.json")
    parser.add_argument("--second-forecast-cadence-sec", type=int, default=1)
    parser.add_argument("--second-forecast-sample-sec", type=int, default=5)
    parser.add_argument("--second-forecast-signal-interval-sec", type=int, default=5)
    parser.add_argument("--promotion-database", type=Path, default=state / "lane_promotion_v1.sqlite")
    parser.add_argument("--promotion-state", type=Path, default=state / "lane_promotion_v1.json")
    parser.add_argument("--exit-fit-database", type=Path, default=state / "strategy_exit_fit_v1.sqlite")
    parser.add_argument("--exit-fit-state", type=Path, default=state / "strategy_exit_fit_v1.json")
    parser.add_argument(
        "--shadow-outcome-database",
        type=Path,
        default=state / "second_forecast_shadow_outcomes_v1.sqlite",
    )
    parser.add_argument("--shadow-outcome-batch-size", type=int, default=4096)
    parser.add_argument("--shadow-outcome-flush-sec", type=float, default=1.0)
    parser.add_argument("--shadow-outcome-log-sample-rate", type=float, default=0.01)
    parser.add_argument("--execution-ranking-horizon-sec", type=int, default=300)
    parser.add_argument(
        "--execution-horizons",
        type=parse_horizons,
        default=list(DEFAULT_EXECUTION_HORIZONS_SEC),
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
    parser.add_argument("--execution-units", type=int, default=100)
    parser.add_argument("--execution-max-units", type=int, default=5000)
    parser.add_argument("--execution-cooldown-sec", type=float, default=30.0)
    parser.add_argument("--execution-max-hold-sec", type=float, default=300.0)
    parser.add_argument("--execution-max-quote-age-sec", type=float, default=3.0)
    parser.add_argument("--execution-max-slippage-pips", type=float, default=0.5)
    parser.add_argument("--execution-max-loss-account", type=float, default=2.0)
    parser.add_argument("--execution-recent-veto-min-samples", type=int, default=5)
    parser.add_argument("--execution-recent-veto-max-win-rate", type=float, default=40.0)
    parser.add_argument("--execution-min-historical-reliability", type=float, default=0.15)
    parser.add_argument(
        "--execution-allow-unvalidated-signals",
        action=argparse.BooleanOptionalAction,
        default=True,
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
        default=state / "practice_007_signal_feed_v1.sqlite",
    )
    parser.add_argument(
        "--track-shared-forecast-feed",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument(
        "--shared-forecast-ledger",
        type=Path,
        default=state / "all_signal_live_forecasts_v1.sqlite",
    )
    parser.add_argument(
        "--shared-forecast-state",
        type=Path,
        default=state / "all_signal_live_forecasts_v1.json",
    )
    parser.add_argument("--shared-forecast-retention-days", type=int, default=7)
    parser.add_argument("--shared-forecast-scan-sec", type=float, default=0.5)
    parser.add_argument("--shared-forecast-max-delay-sec", type=float, default=5.0)
    parser.add_argument("--shared-forecast-summary-sec", type=float, default=300.0)
    parser.add_argument("--shared-forecast-limit", type=int, default=50000)
    parser.add_argument(
        "--shared-forecast-maturity-batch-size",
        type=int,
        default=5000,
    )
    parser.add_argument(
        "--execution-policy-state",
        type=Path,
        default=state / "practice_007_execution_policy_v1.json",
    )
    parser.add_argument("--execution-feed-source", default="second_forecast_hot")
    parser.add_argument("--execution-signal-feed-ttl-sec", type=float, default=8.0)
    parser.add_argument("--execution-min-signal-confidence", type=float, default=0.54)
    parser.add_argument("--execution-min-signal-expected-net-pips", type=float, default=0.05)
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
    parser.add_argument("--execution-second-curve-min-net-pips", type=float, default=0.05)
    parser.add_argument("--execution-second-curve-min-aligned-fraction", type=float, default=0.5)
    parser.add_argument(
        "--execution-second-curve-entry-veto",
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
    parser.add_argument("--execution-trailing-activation-spread-multiple", type=float, default=0.5)
    parser.add_argument("--execution-profit-lock-trigger-pips", type=float, default=2.0)
    parser.add_argument("--execution-profit-lock-floor-pips", type=float, default=0.2)
    parser.add_argument("--execution-profit-lock-spread-multiple", type=float, default=1.5)
    parser.add_argument("--execution-profit-lock-step-pips", type=float, default=0.25)
    parser.add_argument("--execution-profit-hold-multiplier", type=float, default=2.0)
    parser.add_argument("--execution-dynamic-sizing", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--execution-open-ended-profit", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--execution-manage-trades", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--execution-lock-path", type=Path, default=state / "practice_007_order.lock")
    parser.add_argument(
        "--execution-use-fitted-exits",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    args = parser.parse_args(argv)
    numeric = (
        args.duration_sec,
        args.scan_pause_sec,
        args.stream_start_timeout_sec,
        args.stream_stale_sec,
        args.rest_fallback_sec,
        args.second_forecast_cadence_sec,
        args.second_forecast_sample_sec,
        args.second_forecast_signal_interval_sec,
        args.execution_min_samples,
        args.execution_units,
        args.execution_max_units,
        args.execution_max_hold_sec,
        args.execution_max_quote_age_sec,
        args.shared_forecast_retention_days,
        args.shared_forecast_scan_sec,
        args.shared_forecast_max_delay_sec,
        args.shared_forecast_summary_sec,
        args.shared_forecast_limit,
        args.shared_forecast_maturity_batch_size,
    )
    if any(float(value) <= 0.0 for value in numeric):
        raise SystemExit("duration, stream, forecast, and execution values must be positive")
    if (
        not 0.5 <= args.execution_min_signal_confidence < 1.0
        or args.execution_prediction_quality_min_samples <= 0
        or not 0.0 <= args.execution_prediction_quality_max_weight <= 1.0
        or args.execution_second_curve_min_net_pips < 0.0
        or not 0.0 <= args.execution_second_curve_min_aligned_fraction <= 1.0
        or args.execution_max_open_positions <= 0
        or not 0.0 < args.execution_max_currency_direction_margin_pct <= 100.0
        or not 0.0 < args.execution_target_margin_used_pct < args.execution_hard_margin_used_pct < 100.0
        or not args.execution_target_margin_used_pct <= args.execution_high_confidence_margin_used_pct <= args.execution_hard_margin_used_pct
        or args.execution_min_trailing_pips <= 0.0
        or args.execution_trailing_spread_multiple <= 0.0
        or args.execution_trailing_stop_r <= 0.0
        or args.execution_trailing_activation_r <= 0.0
        or args.execution_trailing_activation_spread_multiple < 0.0
        or args.execution_profit_lock_trigger_pips <= 0.0
        or args.execution_profit_lock_floor_pips < 0.0
        or args.execution_profit_lock_spread_multiple <= 0.0
        or args.execution_profit_lock_step_pips <= 0.0
        or args.execution_recent_veto_min_samples <= 0
        or not 0.0 <= args.execution_recent_veto_max_win_rate <= 100.0
        or not 0.0 <= args.execution_min_historical_reliability <= 1.0
        or args.execution_paper_consensus_min_families < 2
        or not 50.0 <= args.execution_paper_consensus_min_aligned_weight_pct <= 100.0
        or not 0.5 <= args.execution_paper_consensus_min_confidence < 1.0
        or args.execution_paper_consensus_min_net_pips < 0.0
        or args.execution_paper_consensus_min_gross_to_spread < 1.0
        or args.execution_paper_consensus_max_horizon_sec <= 0
        or args.shadow_outcome_batch_size <= 0
        or args.shadow_outcome_flush_sec <= 0.0
        or not 0.0 <= args.shadow_outcome_log_sample_rate <= 1.0
    ):
        raise SystemExit("invalid signal-confidence or margin configuration")
    return args


def compact_runtime_summary(summary: dict[str, Any]) -> dict[str, Any]:
    """Keep heartbeat logs useful without duplicating persisted matrices."""

    compact = dict(summary)
    forecast_curves = compact.pop("forecast_curves", [])
    smoothing = compact.pop("multi_horizon_smoothing", {})
    compact["forecast_curve_pairs"] = len(forecast_curves)
    compact["smoothing_pair_records"] = len(
        smoothing.get("pair_diagnostics") or []
        if isinstance(smoothing, dict)
        else []
    )
    return compact


def shared_forecast_snapshot(
    prices: dict[str, Any],
    observed_epoch: float,
) -> dict[str, Any]:
    return {
        "generated_epoch": float(observed_epoch),
        "instruments": {
            instrument: {
                "quote": {
                    "bid": float(quote.bid),
                    "ask": float(quote.ask),
                    "time": str(getattr(quote, "time", "") or ""),
                }
            }
            for instrument, quote in prices.items()
            if quote is not None
            and math.isfinite(float(quote.bid))
            and math.isfinite(float(quote.ask))
            and float(quote.bid) > 0.0
            and float(quote.ask) >= float(quote.bid)
        },
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    args.log_dir.mkdir(parents=True, exist_ok=True)
    token, account_id = read_credentials(args.creds, args.account_key, args.account_id)
    instruments = tradeable_currency_instruments(token, account_id)
    client = MarketDataClient(token)
    pip_sizes = account_pip_sizes(client, account_id)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    log_path = args.log_dir / f"second_forecast_{args.runner_role}_{account_id[-4:]}_{stamp}.jsonl"
    heartbeat_path = args.heartbeat_state or (
        DEFAULT_LOG_DIR.parent
        / "state"
        / f"second_forecast_{args.runner_role}_heartbeat_v1.json"
    )
    heartbeat = WorkerHeartbeat(
        heartbeat_path,
        worker="oanda_second_forecast_runner",
        role=args.runner_role,
    ).start()
    heartbeat.update(
        phase="initializing",
        account_suffix=account_id[-4:],
        instrument_count=len(instruments),
    )
    performance = LanePerformance(args.execution_ranking_horizon_sec)
    exit_fit = StrategyExitFit(
        args.exit_fit_database,
        args.exit_fit_state,
        horizon_sec=args.execution_ranking_horizon_sec,
        horizons_sec=args.execution_horizons,
        fit_enabled=False,
        record_enabled=args.runner_role != "microstructure",
    )
    outcome_store = (
        ShadowOutcomeStore(
            args.shadow_outcome_database,
            batch_size=args.shadow_outcome_batch_size,
            flush_sec=args.shadow_outcome_flush_sec,
        )
        if args.runner_role == "tracker"
        else None
    )
    shared_feed = (
        SignalContributionFeed(args.execution_signal_feed_database)
        if args.runner_role == "tracker" and args.track_shared_forecast_feed
        else None
    )
    shared_ledger = (
        LiveForecastLedger(
            args.shared_forecast_ledger,
            args.shared_forecast_retention_days,
        )
        if shared_feed is not None
        else None
    )
    promotion_store = (
        LanePromotionStore(args.promotion_database)
        if args.runner_role == "tracker"
        else None
    )
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
    executor: PracticeExecutor | None = None
    if args.runner_role == "hot":
        executor = PracticeExecutor(client, account_id, log_path, performance, args, exit_fit, promotion)
        executor.initialize()
    runtime = SecondForecastRuntime(
        args.second_forecast_model,
        args.second_forecast_database,
        args.second_forecast_state,
        cadence_sec=args.second_forecast_cadence_sec,
        sample_sec=args.second_forecast_sample_sec,
        signal_interval_sec=args.second_forecast_signal_interval_sec,
        execution_horizons=set(args.execution_horizons),
        research_enabled=args.runner_role in {"tracker", "microstructure"},
        shadow_signal_tracking_enabled=args.runner_role == "tracker",
    )
    stream = MultiPriceStream(
        lambda: read_credentials(args.creds, args.account_key, args.account_id),
        instruments,
        lambda event, **fields: log_line(log_path, event, **fields),
    )
    if executor is not None:
        executor.set_price_snapshot_provider(stream.snapshot)
    stream.start()
    ready = stream.wait_ready(args.stream_start_timeout_sec)
    log_line(
        log_path,
        "second_runner_start",
        runner_role=args.runner_role,
        account_suffix=account_id[-4:],
        stream_ready=ready,
        instruments=len(instruments),
        model_ready=runtime.ready,
        model_pairs=len(runtime.model.state.get("models") or {}),
        matrix_id="unified_forecast_matrix_v1",
        model_family=MODEL_FAMILY,
        input_timeframe=INPUT_TIMEFRAME,
        training_timeframe=TRAINING_TIMEFRAME,
        outcome_horizons=runtime.summary().get("outcome_horizons_sec") or [],
        cadence_sec=args.second_forecast_cadence_sec,
        execution_horizons=args.execution_horizons,
        promotion_source_complete=bool(promotion.state.get("source_complete")),
        shadow_outcomes={} if outcome_store is None else {
            **outcome_store.summary(),
            "detail_log_sample_rate": args.shadow_outcome_log_sample_rate,
        },
        shared_forecast_tracking=(
            {}
            if shared_ledger is None
            else {
                "feed": str(args.execution_signal_feed_database.resolve()),
                "ledger": str(args.shared_forecast_ledger.resolve()),
                "state": str(args.shared_forecast_state.resolve()),
                "retention_days": args.shared_forecast_retention_days,
                "scan_sec": args.shared_forecast_scan_sec,
                "max_delay_sec": args.shared_forecast_max_delay_sec,
            }
        ),
        heartbeat_state=str(heartbeat_path.resolve()),
    )
    stop_at = time.monotonic() + args.duration_sec
    pending: list[dict[str, Any]] = []
    prices: dict[str, Any] = {}
    last_rest = -math.inf
    next_tick = 0.0
    cycles = 0
    outcomes = 0
    errors = 0
    shared_registered = 0
    shared_expired_skipped = 0
    shared_matured = 0
    shared_censored = 0
    shared_errors = 0
    shared_register_seconds = 0.0
    shared_mature_seconds = 0.0
    outcome_mature_seconds = 0.0
    shared_seen: set[str] = set()
    shared_seen_order: deque[str] = deque()
    shared_summary: dict[str, Any] = {}
    shared_pruned = 0
    next_shared_scan = -math.inf
    next_shared_summary = -math.inf
    next_shared_prune = time.monotonic() + 3600.0
    try:
        while time.monotonic() < stop_at:
            try:
                streamed = stream.snapshot()
                complete = len(streamed) >= max(1, math.ceil(len(instruments) * 0.95))
                if stream.healthy(args.stream_stale_sec) and complete:
                    prices = streamed
                elif time.monotonic() - last_rest >= args.rest_fallback_sec:
                    prices = merge_price_maps(client.pricing_snapshot(account_id, instruments), streamed)
                    last_rest = time.monotonic()
                else:
                    prices = merge_price_maps(prices, streamed)
                if prices:
                    heartbeat.update(
                        phase="forecasting",
                        priced_instruments=len(prices),
                        pending=len(pending),
                    )
                    runtime.evaluate(
                        prices,
                        stream.snapshot_metadata(),
                        pip_sizes,
                        pending,
                        lambda event, **fields: log_line(log_path, event, **fields),
                        candidate_callback=None if executor is None else executor.maybe_execute,
                    )
                    if (
                        shared_feed is not None
                        and shared_ledger is not None
                        and time.monotonic() >= next_shared_scan
                    ):
                        try:
                            observed_epoch = time.time()
                            recent = shared_feed.recent(
                                args.shared_forecast_limit
                            )
                            new_candidates: list[dict[str, Any]] = []
                            for candidate in recent:
                                candidate_id = str(candidate.get("id") or "")
                                if (
                                    not candidate_id
                                    or candidate_id in shared_seen
                                    or not candidate.get("forecast_curve")
                                ):
                                    continue
                                shared_seen.add(candidate_id)
                                shared_seen_order.append(candidate_id)
                                new_candidates.append(candidate)
                            while len(shared_seen_order) > 200000:
                                shared_seen.discard(
                                    shared_seen_order.popleft()
                                )
                            register_started = time.monotonic()
                            shared_registered += shared_ledger.register(
                                new_candidates,
                                f"shared-{int(observed_epoch * 1000)}",
                                minimum_target_epoch=(
                                    observed_epoch
                                    - args.shared_forecast_max_delay_sec
                                ),
                            )
                            shared_expired_skipped += int(
                                shared_ledger.last_register_stats.get(
                                    "expired_points_skipped",
                                    0,
                                )
                            )
                            shared_register_seconds = (
                                time.monotonic() - register_started
                            )
                            mature_started = time.monotonic()
                            result = shared_ledger.mature(
                                shared_forecast_snapshot(
                                    prices,
                                    observed_epoch,
                                ),
                                args.shared_forecast_max_delay_sec,
                                batch_size=(
                                    args.shared_forecast_maturity_batch_size
                                ),
                            )
                            shared_mature_seconds = (
                                time.monotonic() - mature_started
                            )
                            shared_matured += result["matured"]
                            shared_censored += result["censored"]
                            if time.monotonic() >= next_shared_summary:
                                shared_summary = shared_ledger.summary()
                                next_shared_summary = (
                                    time.monotonic()
                                    + args.shared_forecast_summary_sec
                                )
                            if time.monotonic() >= next_shared_prune:
                                shared_pruned += shared_ledger.prune(
                                    observed_epoch
                                )
                                next_shared_prune = (
                                    time.monotonic() + 3600.0
                                )
                        except Exception as exc:
                            shared_errors += 1
                            log_line(
                                log_path,
                                "shared_forecast_tracker_error",
                                error_type=type(exc).__name__,
                                error=str(exc)[:500],
                            )
                        finally:
                            next_shared_scan = (
                                time.monotonic()
                                + args.shared_forecast_scan_sec
                            )
                    if args.runner_role == "tracker":
                        heartbeat.update(
                            phase="maturing_outcomes",
                            pending=len(pending),
                        )
                        outcome_mature_started = time.monotonic()
                        outcomes += process_pending(
                            prices,
                            pending,
                            log_path,
                            performance,
                            exit_fit,
                            promotion_store,
                            outcome_store,
                            args.shadow_outcome_log_sample_rate,
                        )
                        outcome_mature_seconds = (
                            time.monotonic() - outcome_mature_started
                        )
                    elif executor is not None:
                        executor.manage_open_trades()
                cycles += 1
                if time.monotonic() >= next_tick:
                    if shared_ledger is not None:
                        shared_summary = {
                            **shared_summary,
                            "database": str(
                                args.shared_forecast_ledger.resolve()
                            ),
                            "pending": shared_ledger.pending_predictions,
                        }
                    log_line(
                        log_path,
                        "second_runner_tick",
                        cycles=cycles,
                        priced_instruments=len(prices),
                        pending=len(pending),
                        outcomes=outcomes,
                        runtime=compact_runtime_summary(runtime.summary()),
                        stream=stream.stats(),
                        shared_forecasts={
                            "registered": shared_registered,
                            "expired_skipped": shared_expired_skipped,
                            "matured": shared_matured,
                            "censored": shared_censored,
                            "pruned": shared_pruned,
                            "errors": shared_errors,
                            "register_seconds": round(
                                shared_register_seconds,
                                6,
                            ),
                            "mature_seconds": round(
                                shared_mature_seconds,
                                6,
                            ),
                            "second_outcome_mature_seconds": round(
                                outcome_mature_seconds,
                                6,
                            ),
                            "summary": shared_summary,
                        },
                        runner_role=args.runner_role,
                        fills=0 if executor is None else executor.fills,
                        execution_disabled_reason="" if executor is None else executor.disabled_reason,
                    )
                    if shared_ledger is not None:
                        atomic_json(
                            args.shared_forecast_state,
                            {
                                "schema_version": 1,
                                "updated_utc": datetime.now(
                                    timezone.utc
                                ).isoformat(),
                                "feed": str(
                                    args.execution_signal_feed_database.resolve()
                                ),
                                "ledger": str(
                                    args.shared_forecast_ledger.resolve()
                                ),
                                "registered_prediction_points": (
                                    shared_registered
                                ),
                                "expired_prediction_points_skipped": (
                                    shared_expired_skipped
                                ),
                                "matured_prediction_points": shared_matured,
                                "censored_prediction_points": shared_censored,
                                "pruned_rows": shared_pruned,
                                "errors": shared_errors,
                                "seen_candidates": len(shared_seen),
                                "performance": {
                                    "maturity_batch_size": (
                                        args.shared_forecast_maturity_batch_size
                                    ),
                                    "last_register_seconds": round(
                                        shared_register_seconds,
                                        6,
                                    ),
                                    "last_mature_seconds": round(
                                        shared_mature_seconds,
                                        6,
                                    ),
                                    "last_second_outcome_mature_seconds": round(
                                        outcome_mature_seconds,
                                        6,
                                    ),
                                },
                                "summary": shared_summary,
                                "contract": {
                                    "scope": (
                                        "all_shared_signal_forecast_curves"
                                    ),
                                    "quote_source": (
                                        "live_oanda_pricing_stream"
                                    ),
                                    "cost_basis": "executable_bid_ask",
                                    "account_execution_authorized": False,
                                },
                            },
                        )
                    runtime_health = runtime.summary()
                    heartbeat.update(
                        phase="streaming",
                        cycles=cycles,
                        priced_instruments=len(prices),
                        pending=len(pending),
                        outcomes=outcomes,
                        errors=errors,
                        shared_forecast_registered=shared_registered,
                        shared_forecast_expired_skipped=(
                            shared_expired_skipped
                        ),
                        shared_forecast_matured=shared_matured,
                        shared_forecast_errors=shared_errors,
                        shared_forecast_register_seconds=round(
                            shared_register_seconds,
                            6,
                        ),
                        shared_forecast_mature_seconds=round(
                            shared_mature_seconds,
                            6,
                        ),
                        outcome_mature_seconds=round(
                            outcome_mature_seconds,
                            6,
                        ),
                        runtime_sampled_forecasts=int(
                            runtime_health.get("sampled_forecasts") or 0
                        ),
                        runtime_matured_outcomes=int(
                            runtime_health.get("matured_outcomes") or 0
                        ),
                        runtime_pending_sampled_outcomes=int(
                            runtime_health.get("pending_sampled_outcomes") or 0
                        ),
                        runtime_latency_ms_p95=round(
                            float(runtime_health.get("latency_ms_p95") or 0.0),
                            3,
                        ),
                        fills=0 if executor is None else executor.fills,
                    )
                    next_tick = time.monotonic() + 10.0
            except OandaApiError as exc:
                errors += 1
                log_line(log_path, "second_runner_api_error", status=exc.status, error=str(exc)[:500])
            except Exception as exc:
                errors += 1
                log_line(log_path, "second_runner_cycle_error", error_type=type(exc).__name__, error=str(exc)[:500])
            time.sleep(min(args.scan_pause_sec, max(0.0, stop_at - time.monotonic())))
    finally:
        stream.stop()
        runtime.close()
        if outcome_store is not None:
            outcome_store.close()
        if shared_ledger is not None:
            shared_ledger.close()
        if shared_feed is not None:
            shared_feed.close()
        exit_fit.close()
        if promotion_store is not None:
            promotion_store.close()
        if executor is not None:
            executor.close()
        heartbeat.close()
        client.close()
    log_line(
        log_path,
        "second_runner_end",
        cycles=cycles,
        outcomes=outcomes,
        errors=errors,
        runner_role=args.runner_role,
        fills=0 if executor is None else executor.fills,
        runtime=runtime.summary(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
