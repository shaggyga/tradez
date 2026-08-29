#!/usr/bin/env python3
"""Relay the consolidated -007 signal matrix into an aggressive practice account.

The heavy strategy laboratory remains the only producer. This worker consumes its
small live snapshot, reprices candidates at OANDA, and delegates order sizing and
exit management to the existing practice executor. It intentionally permits
unvalidated signals for paper-account sample generation, but never signals with
explicit negative-quality evidence.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from oanda_practice_shadow_strategy_lab import (
        DEFAULT_CREDS,
        LanePerformance,
        MarketDataClient,
        PracticeExecutor,
        atomic_json,
        log_line,
        parse_args as strategy_parse_args,
        read_credentials,
        safe_float,
        utc_now,
    )
except ModuleNotFoundError:  # Package imports used by the test suite.
    from trad.oanda_practice_shadow_strategy_lab import (
        DEFAULT_CREDS,
        LanePerformance,
        MarketDataClient,
        PracticeExecutor,
        atomic_json,
        log_line,
        parse_args as strategy_parse_args,
        read_credentials,
        safe_float,
        utc_now,
    )


ROOT = Path(__file__).resolve().parent
STATE_DIR = ROOT / "data" / "oanda_training_manager" / "state"
LOG_DIR = ROOT / "data" / "oanda_training_manager" / "logs"
DEFAULT_SOURCE = STATE_DIR / "practice_007_signal_snapshot_v1.json"
DEFAULT_STATE = STATE_DIR / "practice_006_merged_relay_state_v1.json"
DEFAULT_HEARTBEAT = STATE_DIR / "practice_006_merged_relay_heartbeat_v1.json"
DEFAULT_EXECUTION_DB = STATE_DIR / "practice_006_merged_signal_feed_v1.sqlite"
DEFAULT_LOCK = STATE_DIR / "practice_006_merged_order.lock"
DEFAULT_LOG = LOG_DIR / "practice_006_merged_relay.jsonl"
EXECUTION_HORIZONS = (60, 120, 180, 300, 600, 900, 1800, 3600, 7200, 10800, 14400)


def parse_utc(value: Any) -> datetime | None:
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


def load_json(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return dict(default or {})
    return payload if isinstance(payload, dict) else dict(default or {})


def signal_fingerprint(row: dict[str, Any]) -> str:
    return ":".join(
        (
            str(row.get("instrument") or ""),
            str(row.get("direction") or ""),
            str(int(safe_float(row.get("preferred_horizon_sec") or row.get("execution_horizon_sec")))),
        )
    )


def execution_exit_horizon(horizon_sec: int, maximum_sec: int) -> int:
    cap = max(60, min(int(horizon_sec or 60), int(maximum_sec)))
    eligible = [value for value in EXECUTION_HORIZONS if value <= cap]
    return max(eligible, default=60)


def candidate_rejection_reason(row: dict[str, Any], args: argparse.Namespace) -> str:
    direction = str(row.get("direction") or "").lower()
    if not row.get("instrument") or direction not in {"buy", "sell"}:
        return "invalid_signal_identity"
    confidence = safe_float(row.get("signal_confidence"), 0.0)
    projected_net = safe_float(
        row.get("instant_projected_net_pips"),
        safe_float(row.get("projected_net_pips"), -math.inf),
    )
    agreement = int(safe_float(row.get("agreement_family_count")))
    opposing = int(safe_float(row.get("opposing_family_count")))
    validation = row.get("execution_validation") or {}
    if bool(validation.get("negative_historical_warmup")):
        return "negative_historical_warmup"
    if bool(validation.get("prediction_quality_negative")):
        if not args.allow_negative_quality_exploration:
            return "prediction_quality_negative"
        if confidence < args.negative_quality_min_confidence:
            return "negative_quality_confidence_floor"
        if projected_net < args.negative_quality_min_projected_net_pips:
            return "negative_quality_projected_net_floor"
        if agreement - opposing < args.negative_quality_min_agreement_edge:
            return "negative_quality_consensus_floor"
    if confidence < args.min_confidence:
        return "confidence_below_aggressive_floor"
    if projected_net < args.min_projected_net_pips:
        return "projected_net_below_floor"
    if agreement < args.min_agreement_families:
        return "insufficient_family_agreement"
    if agreement - opposing < args.min_agreement_edge:
        return "family_consensus_not_positive"
    return ""


def candidate_score(row: dict[str, Any]) -> float:
    confidence = safe_float(row.get("signal_confidence"), 0.5)
    projected = max(
        0.0,
        safe_float(
            row.get("instant_projected_net_pips"),
            safe_float(row.get("projected_net_pips")),
        ),
    )
    agreement = max(0.0, safe_float(row.get("agreement_family_count")))
    opposing = max(0.0, safe_float(row.get("opposing_family_count")))
    reliability = max(0.0, safe_float(row.get("historical_reliability")))
    return (
        5.0 * (confidence - 0.5)
        + 0.35 * math.log1p(projected)
        + 0.04 * (agreement - opposing)
        + 0.20 * reliability
    )


def ranked_snapshot_signals(
    snapshot: dict[str, Any],
    args: argparse.Namespace,
) -> tuple[list[dict[str, Any]], Counter[str]]:
    accepted: list[dict[str, Any]] = []
    rejected: Counter[str] = Counter()
    for raw in snapshot.get("top_signals") or []:
        if not isinstance(raw, dict):
            rejected["invalid_signal_row"] += 1
            continue
        reason = candidate_rejection_reason(raw, args)
        if reason:
            rejected[reason] += 1
            continue
        row = dict(raw)
        validation = row.get("execution_validation") or {}
        row["quality_tier"] = (
            "negative_evidence_exploration"
            if bool(validation.get("prediction_quality_negative"))
            else "standard"
        )
        row["merged_rank_score"] = candidate_score(row)
        if row["quality_tier"] == "negative_evidence_exploration":
            row["merged_rank_score"] -= 0.5
        accepted.append(row)
    accepted.sort(
        key=lambda row: (
            safe_float(row.get("merged_rank_score")),
            safe_float(row.get("signal_confidence")),
            safe_float(row.get("projected_net_pips")),
        ),
        reverse=True,
    )
    return accepted, rejected


def build_execution_candidate(
    signal: dict[str, Any],
    quote: Any,
    instrument_meta: dict[str, float],
    args: argparse.Namespace,
) -> tuple[dict[str, Any] | None, str]:
    bid = safe_float(getattr(quote, "bid", 0.0))
    ask = safe_float(getattr(quote, "ask", 0.0))
    if bid <= 0.0 or ask <= bid:
        return None, "invalid_live_quote"
    pip = 10.0 ** int(safe_float(instrument_meta.get("pip_location"), -4.0))
    spread_pips = (ask - bid) / max(pip, 1e-12)
    if spread_pips > args.max_spread_pips:
        return None, "spread_above_absolute_cap"
    projected = max(
        0.0,
        safe_float(
            signal.get("instant_projected_net_pips"),
            safe_float(signal.get("projected_net_pips")),
        ),
    )
    stop_pips = min(
        args.max_stop_pips,
        max(
            args.min_stop_pips,
            spread_pips * args.stop_spread_multiple,
            projected * args.stop_projected_multiple,
        ),
    )
    if spread_pips / max(stop_pips, 1e-9) > args.max_spread_to_stop_ratio:
        return None, "spread_to_stop_ratio"
    horizon = int(
        safe_float(
            signal.get("preferred_horizon_sec"),
            safe_float(signal.get("execution_horizon_sec"), 300.0),
        )
    )
    fingerprint = signal_fingerprint(signal)
    source_lane = str(signal.get("lane_id") or signal.get("family") or "signal")
    quality_tier = str(signal.get("quality_tier") or "standard")
    lane_prefix = "merged007.explore" if quality_tier == "negative_evidence_exploration" else "merged007"
    return {
        "id": f"merged006:{fingerprint}:{int(time.time())}",
        "lane_id": f"{lane_prefix}.{source_lane}"[:72],
        "family": "merged_007_consensus",
        "profile": "aggressive",
        "model_id": str(signal.get("id") or source_lane),
        "input_timeframe": "multi",
        "instrument": str(signal.get("instrument") or ""),
        "direction": str(signal.get("direction") or "").lower(),
        "bid": bid,
        "ask": ask,
        "pip": pip,
        "entry_time": str(getattr(quote, "time", "") or utc_now()),
        "forecast_created_monotonic": time.monotonic(),
        "execution_horizon_sec": horizon,
        "execution_exit_horizon_sec": execution_exit_horizon(horizon, args.max_hold_sec),
        "stop_loss_pips": round(stop_pips, 3),
        "take_profit_r": args.take_profit_r,
        "open_ended_profit": True,
        "spread_pips": round(spread_pips, 4),
        "signal_strength_pips": round(projected + spread_pips, 4),
        "signal_to_spread": (projected + spread_pips) / max(spread_pips, 1e-9),
        "reward_to_spread": stop_pips * args.take_profit_r / max(spread_pips, 1e-9),
        "signal_confidence": safe_float(signal.get("signal_confidence"), 0.5),
        "signal_score": safe_float(signal.get("merged_rank_score")),
        "projected_net_pips": projected,
        "instant_projected_net_pips": signal.get("instant_projected_net_pips"),
        "historical_expected_net_pips": signal.get("historical_expected_net_pips"),
        "historical_reliability": signal.get("historical_reliability"),
        "agreement_family_count": signal.get("agreement_family_count"),
        "opposing_family_count": signal.get("opposing_family_count"),
        "component_family_count": signal.get("component_family_count"),
        "component_models": signal.get("component_models") or [],
        "source_signal_id": signal.get("id"),
        "source_fingerprint": fingerprint,
        "quality_tier": quality_tier,
        "execution_validation": signal.get("execution_validation") or {},
    }, ""


def configure_executor_args(args: argparse.Namespace) -> argparse.Namespace:
    executor_args = strategy_parse_args([])
    executor_args.execution_signal_feed_database = args.execution_database
    executor_args.execution_policy_state = None
    executor_args.execution_lock_path = args.execution_lock
    executor_args.execution_feed_source = "merged_007_relay"
    executor_args.execution_signal_snapshot = None
    executor_args.execution_cooldown_sec = args.execution_cooldown_sec
    executor_args.execution_max_hold_sec = float(args.max_hold_sec)
    executor_args.execution_max_quote_age_sec = args.max_quote_age_sec
    executor_args.execution_max_slippage_pips = args.max_slippage_pips
    executor_args.execution_max_loss_account = args.max_loss_account
    executor_args.execution_min_signal_confidence = args.min_confidence
    executor_args.execution_min_signal_expected_net_pips = args.min_projected_net_pips
    executor_args.execution_allow_unvalidated_signals = True
    executor_args.execution_prediction_quality = False
    executor_args.execution_prediction_quality_negative_veto = True
    executor_args.execution_second_curve_entry_veto = False
    executor_args.execution_second_curve_profit_exit = False
    executor_args.execution_dynamic_sizing = True
    executor_args.execution_units = 100
    executor_args.execution_max_units = args.max_units
    executor_args.execution_max_open_positions = args.max_open_positions
    executor_args.execution_max_currency_direction_positions = args.max_currency_direction_positions
    executor_args.execution_max_currency_direction_margin_pct = args.max_currency_direction_margin_pct
    executor_args.execution_target_margin_used_pct = args.target_margin_used_pct
    executor_args.execution_high_confidence_margin_used_pct = args.high_confidence_margin_used_pct
    executor_args.execution_hard_margin_used_pct = args.hard_margin_used_pct
    executor_args.execution_min_trade_margin_pct = args.min_trade_margin_pct
    executor_args.execution_max_trade_margin_pct = args.max_trade_margin_pct
    executor_args.execution_min_risk_pct = args.min_risk_pct
    executor_args.execution_max_risk_pct = args.max_risk_pct
    executor_args.execution_min_trailing_pips = args.min_trailing_pips
    executor_args.execution_trailing_spread_multiple = args.trailing_spread_multiple
    executor_args.execution_trailing_stop_r = args.trailing_stop_r
    executor_args.execution_trailing_activation_r = args.trailing_activation_r
    executor_args.execution_trailing_activation_spread_multiple = args.trailing_activation_spread_multiple
    executor_args.execution_profit_lock_trigger_pips = args.profit_lock_trigger_pips
    executor_args.execution_profit_lock_floor_pips = args.profit_lock_floor_pips
    executor_args.execution_profit_lock_spread_multiple = args.profit_lock_spread_multiple
    executor_args.execution_profit_lock_step_pips = args.profit_lock_step_pips
    executor_args.execution_profit_hold_multiplier = args.profit_hold_multiplier
    executor_args.execution_open_ended_profit = True
    executor_args.execution_manage_trades = True
    executor_args.execution_manage_interval_sec = args.manage_interval_sec
    executor_args.execution_use_fitted_exits = False
    executor_args.execution_horizons = list(EXECUTION_HORIZONS)
    return executor_args


def write_runtime_state(
    path: Path,
    *,
    status: str,
    account_id: str,
    source_age_sec: float | None,
    accepted: list[dict[str, Any]],
    rejected: Counter[str],
    executor: PracticeExecutor,
    attempts: int,
    errors: int,
    last_fill: dict[str, Any] | None,
) -> None:
    atomic_json(
        path,
        {
            "schema_version": 1,
            "updated_at": utc_now(),
            "status": status,
            "pid": os.getpid(),
            "account_suffix": account_id[-4:],
            "source_account_suffix": "-007",
            "source_age_sec": None if source_age_sec is None else round(source_age_sec, 3),
            "eligible_signal_count": len(accepted),
            "top_eligible_signals": [
                {
                    "instrument": row.get("instrument"),
                    "direction": row.get("direction"),
                    "confidence": row.get("signal_confidence"),
                    "horizon_sec": row.get("preferred_horizon_sec") or row.get("execution_horizon_sec"),
                    "projected_net_pips": row.get("projected_net_pips"),
                    "agreement_family_count": row.get("agreement_family_count"),
                    "opposing_family_count": row.get("opposing_family_count"),
                    "merged_rank_score": row.get("merged_rank_score"),
                    "quality_tier": row.get("quality_tier"),
                }
                for row in accepted[:8]
            ],
            "rejection_counts": dict(rejected),
            "execution_attempts": attempts,
            "fills_since_start": executor.fills,
            "errors": errors,
            "disabled_reason": executor.disabled_reason,
            "last_fill": last_fill or {},
        },
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--creds", type=Path, default=DEFAULT_CREDS)
    parser.add_argument("--account-key", default="OANDA_ACCOUNT_ID_DUM3")
    parser.add_argument("--account-id", default="")
    parser.add_argument("--expected-account-suffix", default="-006")
    parser.add_argument("--source-snapshot", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--heartbeat", type=Path, default=DEFAULT_HEARTBEAT)
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG)
    parser.add_argument("--execution-database", type=Path, default=DEFAULT_EXECUTION_DB)
    parser.add_argument("--execution-lock", type=Path, default=DEFAULT_LOCK)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    parser.add_argument("--poll-sec", type=float, default=0.25)
    parser.add_argument("--source-max-age-sec", type=float, default=60.0)
    parser.add_argument("--heartbeat-sec", type=float, default=1.0)
    parser.add_argument("--repeat-signal-sec", type=float, default=90.0)
    parser.add_argument("--failed-attempt-retry-sec", type=float, default=5.0)
    parser.add_argument("--execution-cooldown-sec", type=float, default=2.0)
    parser.add_argument("--min-confidence", type=float, default=0.48)
    parser.add_argument("--min-projected-net-pips", type=float, default=0.05)
    parser.add_argument("--min-agreement-families", type=int, default=3)
    parser.add_argument("--min-agreement-edge", type=int, default=1)
    parser.add_argument(
        "--allow-negative-quality-exploration",
        action=argparse.BooleanOptionalAction,
        default=True,
    )
    parser.add_argument("--negative-quality-min-confidence", type=float, default=0.50)
    parser.add_argument("--negative-quality-min-projected-net-pips", type=float, default=0.10)
    parser.add_argument("--negative-quality-min-agreement-edge", type=int, default=3)
    parser.add_argument("--max-negative-quality-open-positions", type=int, default=2)
    parser.add_argument("--max-open-positions", type=int, default=8)
    parser.add_argument("--max-currency-direction-positions", type=int, default=4)
    parser.add_argument("--max-currency-direction-margin-pct", type=float, default=58.0)
    parser.add_argument("--target-margin-used-pct", type=float, default=68.0)
    parser.add_argument("--high-confidence-margin-used-pct", type=float, default=76.0)
    parser.add_argument("--hard-margin-used-pct", type=float, default=82.0)
    parser.add_argument("--min-trade-margin-pct", type=float, default=8.0)
    parser.add_argument("--max-trade-margin-pct", type=float, default=16.0)
    parser.add_argument("--min-risk-pct", type=float, default=0.6)
    parser.add_argument("--max-risk-pct", type=float, default=1.8)
    parser.add_argument("--max-units", type=int, default=5000)
    parser.add_argument("--max-loss-account", type=float, default=7.5)
    parser.add_argument("--max-hold-sec", type=int, default=900)
    parser.add_argument("--max-quote-age-sec", type=float, default=4.0)
    parser.add_argument("--max-slippage-pips", type=float, default=0.7)
    parser.add_argument("--min-stop-pips", type=float, default=4.0)
    parser.add_argument("--max-stop-pips", type=float, default=60.0)
    parser.add_argument("--stop-spread-multiple", type=float, default=3.5)
    parser.add_argument("--stop-projected-multiple", type=float, default=2.0)
    parser.add_argument("--max-spread-pips", type=float, default=20.0)
    parser.add_argument("--max-spread-to-stop-ratio", type=float, default=0.32)
    parser.add_argument("--take-profit-r", type=float, default=1.2)
    parser.add_argument("--manage-interval-sec", type=float, default=0.25)
    parser.add_argument("--min-trailing-pips", type=float, default=1.5)
    parser.add_argument("--trailing-spread-multiple", type=float, default=1.75)
    parser.add_argument("--trailing-stop-r", type=float, default=0.40)
    parser.add_argument("--trailing-activation-r", type=float, default=0.55)
    parser.add_argument("--trailing-activation-spread-multiple", type=float, default=0.15)
    parser.add_argument("--profit-lock-trigger-pips", type=float, default=1.0)
    parser.add_argument("--profit-lock-floor-pips", type=float, default=0.1)
    parser.add_argument("--profit-lock-spread-multiple", type=float, default=1.25)
    parser.add_argument("--profit-lock-step-pips", type=float, default=0.15)
    parser.add_argument("--profit-hold-multiplier", type=float, default=1.5)
    args = parser.parse_args(argv)
    if args.duration_sec <= 0.0 or args.poll_sec <= 0.0 or args.heartbeat_sec <= 0.0:
        parser.error("duration and polling intervals must be positive")
    if not 0.0 < args.target_margin_used_pct < args.hard_margin_used_pct < 100.0:
        parser.error("margin targets must satisfy 0 < target < hard < 100")
    if not args.target_margin_used_pct <= args.high_confidence_margin_used_pct <= args.hard_margin_used_pct:
        parser.error("high-confidence margin must be between target and hard margin")
    return args


def run(args: argparse.Namespace) -> int:
    token, account_id = read_credentials(args.creds, args.account_key, args.account_id)
    if args.expected_account_suffix and not account_id.endswith(args.expected_account_suffix):
        raise SystemExit(
            f"Resolved account {account_id[-4:]} does not match expected practice suffix "
            f"{args.expected_account_suffix}."
        )
    executor_args = configure_executor_args(args)
    client = MarketDataClient(token)
    executor = PracticeExecutor(
        client,
        account_id,
        args.log,
        LanePerformance(300),
        executor_args,
        exit_fit=None,
        promotion=None,
    )
    executor.initialize()
    attempts = 0
    errors = 0
    last_fill: dict[str, Any] | None = None
    attempted_at: dict[str, float] = {}
    filled_at: dict[str, float] = {}
    started = time.monotonic()
    next_heartbeat = 0.0
    last_accepted: list[dict[str, Any]] = []
    last_rejected: Counter[str] = Counter()
    last_source_age: float | None = None
    log_line(
        args.log,
        "merged_relay_started",
        account_suffix=account_id[-4:],
        source_account_suffix="-007",
        source_snapshot=str(args.source_snapshot.resolve()),
        execution_lock=str(args.execution_lock.resolve()),
        aggressive=True,
        allow_unvalidated=True,
        persistent_negative_history_veto=True,
        bounded_negative_quality_exploration=args.allow_negative_quality_exploration,
        max_negative_quality_open_positions=args.max_negative_quality_open_positions,
        max_open_positions=args.max_open_positions,
        target_margin_used_pct=args.target_margin_used_pct,
        hard_margin_used_pct=args.hard_margin_used_pct,
    )
    try:
        while time.monotonic() - started < args.duration_sec:
            cycle_now = time.monotonic()
            status = "running"
            try:
                executor.manage_open_trades()
                snapshot = load_json(args.source_snapshot)
                updated = parse_utc(snapshot.get("updated_at"))
                last_source_age = (
                    max(0.0, (datetime.now(timezone.utc) - updated).total_seconds())
                    if updated is not None
                    else None
                )
                if last_source_age is None or last_source_age > args.source_max_age_sec:
                    status = "source_stale"
                    last_accepted = []
                    last_rejected = Counter({"source_stale": 1})
                elif executor.disabled_reason:
                    status = "execution_disabled"
                else:
                    last_accepted, last_rejected = ranked_snapshot_signals(snapshot, args)
                    now_epoch = time.time()
                    trades = executor.open_trades()
                    open_instruments = {
                        str(trade.get("instrument") or "") for trade in trades
                    }
                    negative_quality_open = sum(
                        "merged007.explore" in str(
                            (trade.get("clientExtensions") or {}).get("comment") or ""
                        )
                        for trade in trades
                    )
                    shared_fill_age = (
                        executor.signal_feed.seconds_since_last_fill()
                        if executor.signal_feed is not None
                        else math.inf
                    )
                    selectable = []
                    for row in last_accepted:
                        fingerprint = signal_fingerprint(row)
                        if str(row.get("instrument") or "") in open_instruments:
                            continue
                        if (
                            row.get("quality_tier") == "negative_evidence_exploration"
                            and negative_quality_open >= args.max_negative_quality_open_positions
                        ):
                            continue
                        if now_epoch - filled_at.get(fingerprint, -math.inf) < args.repeat_signal_sec:
                            continue
                        if now_epoch - attempted_at.get(fingerprint, -math.inf) < args.failed_attempt_retry_sec:
                            continue
                        selectable.append(row)
                    if selectable and shared_fill_age >= args.execution_cooldown_sec:
                        signal = selectable[0]
                        instrument = str(signal.get("instrument") or "")
                        quotes = client.pricing_snapshot(account_id, [instrument])
                        quote = quotes.get(instrument)
                        candidate, quote_reject = build_execution_candidate(
                            signal,
                            quote,
                            executor.instrument_meta.get(instrument) or {},
                            args,
                        ) if quote is not None else (None, "live_quote_unavailable")
                        fingerprint = signal_fingerprint(signal)
                        attempted_at[fingerprint] = now_epoch
                        if candidate is None:
                            last_rejected[quote_reject] += 1
                        else:
                            descriptor = executor.acquire_execution_lock(timeout_sec=1.0)
                            if descriptor is None:
                                last_rejected["account_execution_lock_timeout"] += 1
                            else:
                                before_fills = executor.fills
                                attempts += 1
                                try:
                                    executor.submit_selected_locked(candidate)
                                finally:
                                    executor.release_execution_lock(descriptor)
                                if executor.fills > before_fills:
                                    filled_at[fingerprint] = time.time()
                                    last_fill = {
                                        "time_utc": utc_now(),
                                        "instrument": candidate["instrument"],
                                        "direction": candidate["direction"],
                                        "confidence": candidate["signal_confidence"],
                                        "prediction_horizon_sec": candidate["execution_horizon_sec"],
                                        "exit_horizon_sec": candidate["execution_exit_horizon_sec"],
                                        "source_signal_id": candidate["source_signal_id"],
                                    }
            except Exception as exc:
                errors += 1
                status = "cycle_error"
                log_line(
                    args.log,
                    "merged_relay_cycle_error",
                    error=f"{type(exc).__name__}: {exc}"[:500],
                )
            if cycle_now >= next_heartbeat:
                write_runtime_state(
                    args.heartbeat,
                    status=status,
                    account_id=account_id,
                    source_age_sec=last_source_age,
                    accepted=last_accepted,
                    rejected=last_rejected,
                    executor=executor,
                    attempts=attempts,
                    errors=errors,
                    last_fill=last_fill,
                )
                next_heartbeat = cycle_now + args.heartbeat_sec
            time.sleep(args.poll_sec)
    finally:
        write_runtime_state(
            args.state,
            status="stopped",
            account_id=account_id,
            source_age_sec=last_source_age,
            accepted=last_accepted,
            rejected=last_rejected,
            executor=executor,
            attempts=attempts,
            errors=errors,
            last_fill=last_fill,
        )
        executor.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
