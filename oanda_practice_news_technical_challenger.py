#!/usr/bin/env python3
"""Fast Practice-006 news-sentiment/technical-timing rotation challenger.

This lane exists to collect *execution* observations without weakening the
governed confirmed-candidate executor.  Fresh official or independently
corroborated news supplies direction.  Persistent technical agreement supplies
entry timing; a later strong technical reversal, broker stop/trailing logic, or
the fifteen-minute horizon supplies exit timing.

The lane is deliberately isolated: OANDA practice endpoint only, account suffix
-006 only, fixed small practice sizing, no duplicate instrument, one attempt per
news episode/pair, and at most eight concurrent positions.  It has no real-money
route and does not promote a research hypothesis.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

try:
    import oanda_practice_shadow_strategy_lab as lab
except ModuleNotFoundError:  # Package import used by the complete test suite.
    from trad import oanda_practice_shadow_strategy_lab as lab


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
LOGS = DATA / "logs"
DEFAULT_WATCHLIST = STATE / "news_technical_watchlist_v1.json"
DEFAULT_STATE = STATE / "practice_006_news_challenger_v1.json"
DEFAULT_HEARTBEAT = STATE / "practice_006_news_challenger_heartbeat_v1.json"
DEFAULT_LEDGER = STATE / "practice_006_news_challenger_v1.sqlite"
DEFAULT_LOG = LOGS / "practice_006_news_challenger_v1.jsonl"
DEFAULT_LOCK = STATE / "practice_006_order.lock"

ALLOWED_ARMS = frozenset(
    {
        "official_release_fast_multileg_h15",
        "news_technical_confirmed",
        "news_technical_reconfirmed_h15",
        "news_magnitude_direction_confirmed_h5",
        "news_magnitude_direction_confirmed_h15",
        "news_magnitude_direction_confirmed_h30",
        "uncorroborated_news_response_h5",
        "uncorroborated_news_response_h15",
        "uncorroborated_news_response_h30",
    }
)
SECONDARY_SOURCE_PREFIXES = (
    "google_news",
    "gdelt",
    "alpha_vantage",
    "finnhub",
)
UTC = timezone.utc


def parse_utc(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def direction(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return "long" if normalized in {"long", "buy"} else "short" if normalized in {"short", "sell"} else "neutral"


def source_is_acceptable(
    row: Mapping[str, Any],
    *,
    allow_uncorroborated_secondary: bool = False,
) -> bool:
    """Classify the bounded source tiers allowed by this practice challenger."""
    source_ids = [
        str(value or "").strip().lower()
        for value in (row.get("news_factor_source_ids") or [])
        if str(value or "").strip()
    ]
    if not source_ids:
        return False
    if bool(row.get("uncorroborated_research")):
        return (
            allow_uncorroborated_secondary
            and all(value.startswith(SECONDARY_SOURCE_PREFIXES) for value in source_ids)
            and int(lab.safe_float(row.get("news_factor_article_count"))) >= 1
        )
    if any(not value.startswith(SECONDARY_SOURCE_PREFIXES) for value in source_ids):
        return True
    return (
        int(lab.safe_float(row.get("news_factor_publisher_count"))) >= 2
        and int(lab.safe_float(row.get("news_factor_article_count"))) >= 2
    )


def rejection_reason(
    row: Mapping[str, Any],
    now: datetime,
    args: argparse.Namespace,
) -> str:
    if str(row.get("arm") or "") not in ALLOWED_ARMS:
        return "arm_not_routable"
    if not source_is_acceptable(
        row,
        allow_uncorroborated_secondary=args.allow_uncorroborated_secondary,
    ):
        return "source_not_official_or_corroborated"
    instrument = str(row.get("instrument") or "")
    news_side = direction(row.get("news_direction") or row.get("direction"))
    technical_side = direction(row.get("technical_direction"))
    if not instrument or news_side == "neutral":
        return "invalid_identity"
    reaction_aligned = str(
        (row.get("initial_reaction") or {}).get("state") or ""
    ) == "aligned"
    official_price_reaction_timing = (
        args.allow_official_price_reaction_timing
        and not bool(row.get("uncorroborated_research"))
        and technical_side == "neutral"
        and reaction_aligned
    )
    if technical_side != news_side and not official_price_reaction_timing:
        return "technical_direction_not_aligned"
    if lab.safe_float(row.get("news_confidence")) < args.min_news_confidence:
        return "news_confidence_below_floor"
    if not official_price_reaction_timing:
        if lab.safe_float(row.get("technical_confidence")) < args.min_technical_confidence:
            return "technical_confidence_below_floor"
        if str(row.get("news_thesis_state") or "") != "price_aligned":
            return "news_thesis_not_price_aligned"
        sma = row.get("persistent_sma_confirmation") or {}
        if str(sma.get("state") or "") != "aligned_persistent":
            return "persistent_technical_not_aligned"
    first_known = parse_utc(row.get("news_factor_first_known_utc"))
    expires = parse_utc(row.get("news_factor_expires_utc"))
    if first_known is None:
        return "news_clock_missing"
    age = (now - first_known).total_seconds()
    if age < -60.0 or age > args.max_news_age_sec:
        return "news_clock_outside_entry_window"
    if expires is not None and now >= expires:
        return "news_thesis_expired"
    quote = row.get("entry_quote") or {}
    if quote.get("fresh") is not True:
        return "watchlist_quote_not_fresh"
    spread = lab.safe_float(quote.get("spread_pips"), math.inf)
    if not math.isfinite(spread) or spread <= 0.0 or spread > args.max_spread_pips:
        return "spread_above_challenger_cap"
    if str((row.get("initial_reaction") or {}).get("state") or "") == "conflicted":
        return "initial_reaction_conflicted"
    return ""


def ranked_rows(
    payload: Mapping[str, Any],
    now: datetime,
    args: argparse.Namespace,
) -> tuple[list[dict[str, Any]], Counter[str]]:
    rejected: Counter[str] = Counter()
    if str(payload.get("status") or "") != "ok":
        return [], Counter({"watchlist_not_ok": 1})
    if str(payload.get("market_state") or "") != "fresh":
        return [], Counter({"watchlist_market_not_fresh": 1})
    accepted: list[dict[str, Any]] = []
    for raw in payload.get("watchlist") or []:
        if not isinstance(raw, dict):
            rejected["invalid_row"] += 1
            continue
        reason = rejection_reason(raw, now, args)
        if reason:
            rejected[reason] += 1
            continue
        row = dict(raw)
        spread = lab.safe_float((row.get("entry_quote") or {}).get("spread_pips"))
        reaction = abs(lab.safe_float((row.get("initial_reaction") or {}).get("signed_move_pips")))
        technical_net = max(0.0, lab.safe_float(row.get("technical_expected_net_pips")))
        row["challenger_rank"] = (
            2.0 * lab.safe_float(row.get("news_confidence"))
            + lab.safe_float(row.get("technical_confidence"))
            + 0.15 * min(10.0, reaction)
            + 0.10 * min(5.0, technical_net)
            - 0.10 * spread
        )
        accepted.append(row)
    # One expression of a news episode/currency factor is enough.  Prefer the
    # strongest alignment and lowest-cost executable pair.
    accepted.sort(
        key=lambda row: (
            lab.safe_float(row.get("challenger_rank")),
            -lab.safe_float((row.get("entry_quote") or {}).get("spread_pips")),
        ),
        reverse=True,
    )
    deduplicated: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in accepted:
        key = (str(row.get("episode_id") or ""), str(row.get("currency") or ""))
        if key in seen:
            rejected["same_factor_episode_deduplicated"] += 1
            continue
        seen.add(key)
        deduplicated.append(row)
    return deduplicated, rejected


def candidate_from_row(
    row: Mapping[str, Any],
    quote: Any,
    instrument_meta: Mapping[str, Any],
    args: argparse.Namespace,
) -> tuple[dict[str, Any] | None, str]:
    bid = lab.safe_float(getattr(quote, "bid", 0.0))
    ask = lab.safe_float(getattr(quote, "ask", 0.0))
    if bid <= 0.0 or ask <= bid:
        return None, "live_quote_invalid"
    pip = 10.0 ** int(lab.safe_float(instrument_meta.get("pip_location"), -4.0))
    spread = (ask - bid) / max(pip, 1e-12)
    if spread > args.max_spread_pips:
        return None, "live_spread_above_challenger_cap"
    reaction = abs(lab.safe_float((row.get("initial_reaction") or {}).get("signed_move_pips")))
    technical_net = max(0.0, lab.safe_float(row.get("technical_expected_net_pips")))
    projected_gross = max(spread * args.min_gross_to_spread, reaction, technical_net + spread)
    projected_net = max(0.0, projected_gross - spread)
    if projected_net < args.min_after_cost_pips:
        return None, "estimated_after_cost_edge_below_floor"
    news_conf = lab.safe_float(row.get("news_confidence"), 0.5)
    technical_conf = lab.safe_float(row.get("technical_confidence"), 0.5)
    combined_conf = max(0.5, min(0.75, math.sqrt(news_conf * technical_conf)))
    stop = min(
        args.max_stop_pips,
        max(args.min_stop_pips, spread * args.stop_spread_multiple, 1.25 * reaction),
    )
    episode = str(row.get("episode_id") or "")
    arm = str(row.get("arm") or "")
    side = (
        "buy"
        if direction(row.get("news_direction") or row.get("direction")) == "long"
        else "sell"
    )
    horizon = max(60, min(args.max_hold_sec, int(lab.safe_float(row.get("horizon_min"), 15)) * 60))
    return {
        "id": f"news006:{episode}:{row.get('instrument')}:{arm}",
        "lane_id": f"news006.{arm}.{episode[-12:]}",
        "family": "news_thesis_technical_timing_challenger",
        "profile": "fast_aggressive_practice_rotation",
        "model_id": str(row.get("cohort_id") or arm),
        "instrument": str(row.get("instrument") or ""),
        "direction": side,
        "bid": bid,
        "ask": ask,
        "pip": pip,
        "entry_time": str(getattr(quote, "time", "") or lab.utc_now()),
        "forecast_created_monotonic": time.monotonic(),
        "execution_horizon_sec": horizon,
        "execution_exit_horizon_sec": horizon,
        "stop_loss_pips": round(stop, 3),
        "take_profit_r": args.take_profit_r,
        "open_ended_profit": True,
        "spread_pips": round(spread, 4),
        "signal_strength_pips": round(projected_gross, 4),
        "signal_to_spread": projected_gross / max(spread, 1e-9),
        "gross_to_spread": projected_gross / max(spread, 1e-9),
        "projected_gross_movement_pips": round(projected_gross, 4),
        "projected_net_pips": round(projected_net, 4),
        "instant_projected_net_pips": round(projected_net, 4),
        "liquidity_quality": 1.0,
        "signal_confidence": combined_conf,
        "signal_score": lab.safe_float(row.get("challenger_rank")),
        "source_news_episode_id": episode,
        "source_news_arm": arm,
        "source_news_cohort_id": str(row.get("cohort_id") or ""),
        "source_news_ids": list(row.get("news_factor_source_ids") or []),
        "source_uncorroborated_secondary": bool(row.get("uncorroborated_research")),
        "source_headlines": list(row.get("headlines") or []),
        "technical_source_id": str(row.get("technical_source_id") or ""),
        "news_confidence": news_conf,
        "technical_confidence": technical_conf,
        "research_only_challenger": True,
    }, ""


class NewsTechnicalPracticeExecutor(lab.PracticeExecutor):
    def owns_trade(self, trade: dict[str, Any]) -> bool:
        extensions = trade.get("clientExtensions") or {}
        return (
            str(extensions.get("tag") or "") == "strategy_lab_top"
            and str(extensions.get("comment") or "").startswith("news006.")
        )

    def final_submission_blocker(
        self,
        candidate: dict[str, Any],
        units: int,
        order_body: dict[str, Any],
    ) -> str:
        if "practice" not in str(lab.BASE_URL).lower():
            return "news_challenger_not_practice_endpoint"
        if not self.account_id.endswith("-006"):
            return "news_challenger_not_account_006"
        if units != 100:
            return "news_challenger_units_not_fixed_100"
        if not str(candidate.get("lane_id") or "").startswith("news006."):
            return "news_challenger_lane_identity_invalid"
        if str(candidate.get("source_news_arm") or "") not in ALLOWED_ARMS:
            return "news_challenger_arm_invalid"
        if (
            bool(candidate.get("source_uncorroborated_secondary"))
            and not bool(
                getattr(
                    self.args,
                    "execution_allow_uncorroborated_secondary",
                    False,
                )
            )
        ):
            return "news_challenger_uncorroborated_source_not_enabled"
        return ""


def configure_executor_args(args: argparse.Namespace) -> argparse.Namespace:
    result = lab.parse_args([])
    result.execution_signal_feed_database = args.signal_feed_database
    result.execution_policy_state = None
    result.execution_lock_path = args.execution_lock
    result.execution_feed_source = "practice_006_news_challenger"
    result.execution_signal_snapshot = None
    result.execution_cooldown_sec = args.execution_cooldown_sec
    result.execution_max_hold_sec = float(args.max_hold_sec)
    result.execution_max_quote_age_sec = args.max_quote_age_sec
    result.execution_max_slippage_pips = args.max_slippage_pips
    result.execution_max_loss_account = args.max_loss_account
    result.execution_min_signal_confidence = 0.50
    result.execution_min_signal_expected_net_pips = args.min_after_cost_pips
    result.execution_allow_unvalidated_signals = True
    result.execution_allow_uncorroborated_secondary = (
        args.allow_uncorroborated_secondary
    )
    result.execution_prediction_quality = False
    result.execution_prediction_quality_negative_veto = False
    result.execution_second_curve_entry_veto = False
    result.execution_second_curve_profit_exit = False
    result.execution_dynamic_sizing = False
    result.execution_units = 100
    result.execution_max_units = 100
    result.execution_max_open_positions = args.max_open_positions
    result.execution_max_currency_direction_positions = args.max_open_positions
    result.execution_max_currency_direction_margin_pct = 20.0
    result.execution_target_margin_used_pct = 20.0
    result.execution_high_confidence_margin_used_pct = 25.0
    result.execution_hard_margin_used_pct = 35.0
    result.execution_min_trade_margin_pct = 0.01
    result.execution_max_trade_margin_pct = 1.0
    result.execution_min_risk_pct = 0.01
    result.execution_max_risk_pct = 0.10
    result.execution_reentry_cooldown_sec = args.reentry_cooldown_sec
    result.execution_instrument_reentry_cooldown_sec = args.reentry_cooldown_sec
    result.execution_jpy_factor_cooldown_sec = args.reentry_cooldown_sec
    result.execution_max_open_jpy_factor_positions = args.max_open_positions
    result.execution_intrahour_cost_gate = True
    result.execution_intrahour_cost_gate_max_horizon_sec = 3600
    result.execution_intrahour_max_spread_pips = args.max_spread_pips
    result.execution_intrahour_min_liquidity_quality = 0.0
    result.execution_intrahour_min_confidence = 0.50
    result.execution_intrahour_min_gross_to_spread = args.min_gross_to_spread
    result.execution_intrahour_min_after_cost_pips = args.min_after_cost_pips
    result.execution_manage_trades = True
    result.execution_manage_interval_sec = args.manage_interval_sec
    result.execution_use_fitted_exits = False
    result.execution_open_ended_profit = True
    result.execution_min_trailing_pips = args.min_trailing_pips
    result.execution_trailing_spread_multiple = args.trailing_spread_multiple
    result.execution_trailing_stop_r = args.trailing_stop_r
    result.execution_trailing_activation_r = args.trailing_activation_r
    result.execution_trailing_activation_spread_multiple = 0.25
    result.execution_profit_lock_trigger_pips = args.profit_lock_trigger_pips
    result.execution_profit_lock_floor_pips = 0.1
    result.execution_profit_lock_spread_multiple = 1.25
    result.execution_profit_lock_step_pips = 0.15
    result.execution_profit_hold_multiplier = 1.0
    result.execution_horizons = [60, 300, 900, 1800]
    return result


def open_ledger(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=5.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS attempts(
            attempt_id TEXT PRIMARY KEY,
            attempted_utc TEXT NOT NULL,
            utc_day TEXT NOT NULL,
            episode_id TEXT NOT NULL,
            instrument TEXT NOT NULL,
            direction TEXT NOT NULL,
            arm TEXT NOT NULL,
            status TEXT NOT NULL,
            fill_count INTEGER NOT NULL,
            payload_json TEXT NOT NULL
        )
        """
    )
    connection.commit()
    return connection


def already_attempted(
    connection: sqlite3.Connection,
    row: Mapping[str, Any],
    *,
    allow_repeat: bool = False,
    retry_sec: float = 10.0,
) -> bool:
    latest = connection.execute(
        "SELECT attempted_utc FROM attempts WHERE episode_id=? AND instrument=? "
        "ORDER BY attempted_utc DESC LIMIT 1",
        (str(row.get("episode_id") or ""), str(row.get("instrument") or "")),
    ).fetchone()
    if latest is None:
        return False
    if not allow_repeat:
        return True
    attempted = parse_utc(latest[0])
    if attempted is None:
        return True
    return (datetime.now(UTC) - attempted).total_seconds() < max(0.0, retry_sec)


def daily_fills(connection: sqlite3.Connection, day: str) -> int:
    return int(connection.execute(
        "SELECT COALESCE(SUM(fill_count),0) FROM attempts WHERE utc_day=?", (day,)
    ).fetchone()[0])


def record_attempt(
    connection: sqlite3.Connection,
    row: Mapping[str, Any],
    status: str,
    filled: bool,
    *,
    allow_repeat: bool = False,
) -> None:
    observed = datetime.now(UTC)
    identifier = f"{row.get('episode_id')}|{row.get('instrument')}"
    if allow_repeat:
        identifier = f"{identifier}|{observed.timestamp():.6f}"
    connection.execute(
        "INSERT OR IGNORE INTO attempts VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            identifier,
            observed.isoformat(),
            observed.date().isoformat(),
            str(row.get("episode_id") or ""),
            str(row.get("instrument") or ""),
            direction(row.get("direction")),
            str(row.get("arm") or ""),
            status,
            int(filled),
            json.dumps(dict(row), sort_keys=True, default=str),
        ),
    )
    connection.commit()


def technical_map(payload: Mapping[str, Any]) -> dict[str, tuple[str, float]]:
    result: dict[str, tuple[str, float]] = {}
    for row in payload.get("watchlist") or []:
        if not isinstance(row, dict):
            continue
        instrument = str(row.get("instrument") or "")
        side = direction(row.get("technical_direction") or (row.get("direction") if row.get("arm") == "technical_only" else ""))
        confidence = lab.safe_float(row.get("technical_confidence"))
        if not instrument or side == "neutral":
            continue
        if confidence >= result.get(instrument, ("neutral", -math.inf))[1]:
            result[instrument] = (side, confidence)
    return result


def close_technical_invalidations(
    executor: NewsTechnicalPracticeExecutor,
    payload: Mapping[str, Any],
    args: argparse.Namespace,
) -> int:
    current = technical_map(payload)
    closed = 0
    now = datetime.now(UTC)
    for trade in executor.open_trades():
        if not executor.owns_trade(trade):
            continue
        opened = lab.parse_rfc3339(str(trade.get("openTime") or ""))
        if opened is None or (now - opened).total_seconds() < args.min_technical_exit_age_sec:
            continue
        instrument = str(trade.get("instrument") or "")
        current_side, confidence = current.get(instrument, ("neutral", 0.0))
        trade_side = "long" if lab.safe_float(trade.get("currentUnits")) > 0.0 else "short"
        if current_side == "neutral" or current_side == trade_side or confidence < args.technical_exit_confidence:
            continue
        descriptor = executor.acquire_execution_lock(timeout_sec=0.5)
        if descriptor is None:
            continue
        try:
            response = executor.client.write(
                "PUT",
                f"/v3/accounts/{executor.account_id}/trades/{trade.get('id')}/close",
                {"units": "ALL"},
            )
            fill = response.get("orderFillTransaction") or {}
            lab.log_line(
                executor.log_path,
                "news_challenger_technical_exit",
                trade_id=str(trade.get("id") or ""),
                instrument=instrument,
                trade_direction=trade_side,
                technical_direction=current_side,
                technical_confidence=confidence,
                realized_pl=lab.safe_float(fill.get("pl")),
                request_id=response.get("_requestID"),
            )
            closed += 1
        except lab.OandaApiError as exc:
            lab.log_line(
                executor.log_path,
                "news_challenger_technical_exit_error",
                trade_id=str(trade.get("id") or ""),
                instrument=instrument,
                error=str(exc)[:500],
                outcome_uncertain=exc.outcome_uncertain,
            )
        finally:
            executor.release_execution_lock(descriptor)
    return closed


def write_state(
    path: Path,
    *,
    status: str,
    account_id: str,
    accepted: list[dict[str, Any]],
    rejected: Counter[str],
    executor: NewsTechnicalPracticeExecutor,
    connection: sqlite3.Connection,
    attempts: int,
    technical_exits: int,
    errors: int,
    args: argparse.Namespace,
    account_summary: Mapping[str, Any],
    open_trades: list[dict[str, Any]],
) -> None:
    day = datetime.now(UTC).date().isoformat()
    lab.atomic_json(
        path,
        {
            "schema_version": 1,
            "updated_at": lab.utc_now(),
            "status": status,
            "pid": os.getpid(),
            "environment": "practice",
            "account_suffix": account_id[-4:],
            "account": {
                "balance": lab.safe_float(account_summary.get("balance")),
                "nav": lab.safe_float(account_summary.get("NAV")),
                "cumulative_pl": lab.safe_float(account_summary.get("pl")),
                "unrealized_pl": lab.safe_float(account_summary.get("unrealizedPL")),
                "margin_used": lab.safe_float(account_summary.get("marginUsed")),
                "margin_available": lab.safe_float(account_summary.get("marginAvailable")),
                "open_trade_count": len(open_trades),
                "owned_open_trade_count": sum(
                    1 for trade in open_trades if executor.owns_trade(trade)
                ),
                "pending_order_count": int(
                    lab.safe_float(account_summary.get("pendingOrderCount"))
                ),
                "positions": [
                    {
                        "trade_id": str(trade.get("id") or ""),
                        "instrument": str(trade.get("instrument") or ""),
                        "direction": (
                            "long"
                            if lab.safe_float(trade.get("currentUnits")) > 0.0
                            else "short"
                        ),
                        "units": abs(lab.safe_float(trade.get("currentUnits"))),
                        "entry_price": lab.safe_float(trade.get("price")),
                        "unrealized_pl": lab.safe_float(trade.get("unrealizedPL")),
                        "owned": executor.owns_trade(trade),
                        "stop_loss_order_id": str(
                            (trade.get("stopLossOrder") or {}).get("id") or ""
                        ),
                        "take_profit_order_id": str(
                            (trade.get("takeProfitOrder") or {}).get("id") or ""
                        ),
                    }
                    for trade in open_trades
                ],
            },
            "policy": {
                "research_only_challenger": True,
                "real_money_route": False,
                "direction_source": (
                    "fresh_news_sentiment_with_labeled_secondary_practice_arm"
                    if args.allow_uncorroborated_secondary
                    else "fresh_official_or_corroborated_news"
                ),
                "allow_uncorroborated_secondary": args.allow_uncorroborated_secondary,
                "entry_timing": (
                    "official_price_reaction_or_persistent_technical_alignment"
                    if args.allow_official_price_reaction_timing
                    else "persistent_technical_alignment"
                ),
                "allow_official_price_reaction_timing": (
                    args.allow_official_price_reaction_timing
                ),
                "allow_repeat_episode_pair": args.allow_repeat_episode_pair,
                "attempt_retry_sec": args.attempt_retry_sec,
                "exit_timing": "technical_reversal_or_broker_protection_or_h15",
                "fixed_units": 100,
                "max_total_open_positions": args.max_open_positions,
                "max_daily_fills": args.max_daily_fills,
                "reentry_cooldown_sec": args.reentry_cooldown_sec,
                "core_governed_executor_unchanged": True,
            },
            "eligible_count": len(accepted),
            "eligible_preview": [
                {
                    "arm": row.get("arm"),
                    "episode_id": row.get("episode_id"),
                    "instrument": row.get("instrument"),
                    "direction": row.get("direction"),
                    "news_confidence": row.get("news_confidence"),
                    "technical_confidence": row.get("technical_confidence"),
                    "headlines": list(row.get("headlines") or [])[:2],
                }
                for row in accepted[:5]
            ],
            "rejection_counts": dict(rejected),
            "attempts_since_start": attempts,
            "fills_since_start": executor.fills,
            "fills_today_utc": daily_fills(connection, day),
            "technical_exits_since_start": technical_exits,
            "errors_since_start": errors,
            "disabled_reason": executor.disabled_reason,
        },
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--creds", type=Path, default=lab.DEFAULT_CREDS)
    parser.add_argument("--account-key", default="OANDA_ACCOUNT_ID_DUM3")
    parser.add_argument("--account-id", default="")
    parser.add_argument("--watchlist", type=Path, default=DEFAULT_WATCHLIST)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--heartbeat", type=Path, default=DEFAULT_HEARTBEAT)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG)
    parser.add_argument("--signal-feed-database", type=Path, default=STATE / "practice_006_news_challenger_signal_feed_v1.sqlite")
    parser.add_argument("--execution-lock", type=Path, default=DEFAULT_LOCK)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    parser.add_argument("--poll-sec", type=float, default=1.0)
    parser.add_argument("--heartbeat-sec", type=float, default=2.0)
    parser.add_argument("--max-watchlist-age-sec", type=float, default=180.0)
    parser.add_argument("--max-news-age-sec", type=float, default=3600.0)
    parser.add_argument("--min-news-confidence", type=float, default=0.35)
    parser.add_argument("--allow-uncorroborated-secondary", action="store_true")
    parser.add_argument("--allow-official-price-reaction-timing", action="store_true")
    parser.add_argument("--allow-repeat-episode-pair", action="store_true")
    parser.add_argument("--attempt-retry-sec", type=float, default=10.0)
    parser.add_argument("--min-technical-confidence", type=float, default=0.48)
    parser.add_argument("--technical-exit-confidence", type=float, default=0.56)
    parser.add_argument("--min-technical-exit-age-sec", type=float, default=60.0)
    parser.add_argument("--max-spread-pips", type=float, default=5.0)
    parser.add_argument("--min-gross-to-spread", type=float, default=1.05)
    parser.add_argument("--min-after-cost-pips", type=float, default=0.05)
    parser.add_argument("--max-hold-sec", type=int, default=900)
    parser.add_argument("--max-open-positions", type=int, default=8)
    parser.add_argument("--max-daily-fills", type=int, default=48)
    parser.add_argument("--reentry-cooldown-sec", type=float, default=0.0)
    parser.add_argument("--execution-cooldown-sec", type=float, default=5.0)
    parser.add_argument("--max-quote-age-sec", type=float, default=4.0)
    parser.add_argument("--max-slippage-pips", type=float, default=0.7)
    parser.add_argument("--max-loss-account", type=float, default=1.0)
    parser.add_argument("--min-stop-pips", type=float, default=4.0)
    parser.add_argument("--max-stop-pips", type=float, default=15.0)
    parser.add_argument("--stop-spread-multiple", type=float, default=3.0)
    parser.add_argument("--take-profit-r", type=float, default=1.5)
    parser.add_argument("--manage-interval-sec", type=float, default=0.25)
    parser.add_argument("--min-trailing-pips", type=float, default=1.5)
    parser.add_argument("--trailing-spread-multiple", type=float, default=1.75)
    parser.add_argument("--trailing-stop-r", type=float, default=0.45)
    parser.add_argument("--trailing-activation-r", type=float, default=0.70)
    parser.add_argument("--profit-lock-trigger-pips", type=float, default=1.5)
    args = parser.parse_args(argv)
    if (
        args.max_daily_fills < 1
        or not 1 <= args.max_open_positions <= 8
        or args.max_hold_sec <= 0
        or args.poll_sec <= 0
    ):
        parser.error("fills, one-to-eight open positions, hold, and poll values must be positive")
    return args


def run(args: argparse.Namespace) -> int:
    if "practice" not in str(lab.BASE_URL).lower():
        raise SystemExit("News challenger is restricted to the OANDA practice endpoint.")
    token, account_id = lab.read_credentials(args.creds, args.account_key, args.account_id)
    if not account_id.endswith("-006"):
        raise SystemExit("News challenger is restricted to Practice account suffix -006.")
    executor = NewsTechnicalPracticeExecutor(
        lab.MarketDataClient(token),
        account_id,
        args.log,
        lab.LanePerformance(60),
        configure_executor_args(args),
        exit_fit=None,
        promotion=None,
    )
    executor.initialize()
    ledger = open_ledger(args.ledger)
    attempts = 0
    technical_exits = 0
    errors = 0
    accepted: list[dict[str, Any]] = []
    rejected: Counter[str] = Counter()
    trades: list[dict[str, Any]] = []
    summary: dict[str, Any] = {}
    next_heartbeat = 0.0
    started = time.monotonic()
    lab.log_line(
        args.log,
        "news_challenger_started",
        account_suffix=account_id[-4:],
        fixed_units=100,
        max_open_positions=args.max_open_positions,
        max_daily_fills=args.max_daily_fills,
        core_governed_executor_unchanged=True,
    )
    try:
        while time.monotonic() - started < args.duration_sec:
            status = "running"
            try:
                payload = load_json(args.watchlist)
                generated = parse_utc(payload.get("generated_utc"))
                now = datetime.now(UTC)
                age = math.inf if generated is None else max(0.0, (now - generated).total_seconds())
                executor.manage_open_trades()
                technical_exits += close_technical_invalidations(executor, payload, args)
                if age > args.max_watchlist_age_sec:
                    status = "watchlist_stale"
                    accepted, rejected = [], Counter({"watchlist_stale": 1})
                else:
                    accepted, rejected = ranked_rows(payload, now, args)
                    day = now.date().isoformat()
                    if daily_fills(ledger, day) >= args.max_daily_fills:
                        status = "daily_fill_cap"
                        accepted = []
                        rejected["daily_fill_cap"] += 1
                    trades = executor.open_trades()
                    selectable = [
                        row
                        for row in accepted
                        if not already_attempted(
                            ledger,
                            row,
                            allow_repeat=args.allow_repeat_episode_pair,
                            retry_sec=args.attempt_retry_sec,
                        )
                    ]
                    summary = executor.client.get(
                        f"/v3/accounts/{account_id}/summary", params={}
                    ).get("account") or {}
                    current_balance = lab.safe_float(summary.get("balance"))
                    if (
                        executor.start_balance - current_balance
                        >= args.max_loss_account
                    ):
                        executor.disabled_reason = "news_challenger_session_loss_limit"
                        status = "session_loss_limit"
                        rejected["session_loss_limit"] += len(selectable)
                        selectable = []
                    open_instruments = {
                        str(trade.get("instrument") or "") for trade in trades
                    }
                    selectable = [
                        row
                        for row in selectable
                        if str(row.get("instrument") or "") not in open_instruments
                    ]
                    if len(trades) >= args.max_open_positions:
                        rejected["account_position_cap_reached"] += len(selectable)
                        selectable = []
                    if selectable:
                        row = selectable[0]
                        instrument = str(row.get("instrument") or "")
                        live_quote = executor.client.pricing_snapshot(account_id, [instrument]).get(instrument)
                        candidate, reason = candidate_from_row(
                            row,
                            live_quote,
                            executor.instrument_meta.get(instrument) or {},
                            args,
                        ) if live_quote is not None else (None, "live_quote_missing")
                        if candidate is None:
                            rejected[reason] += 1
                        else:
                            descriptor = executor.acquire_execution_lock(timeout_sec=0.5)
                            if descriptor is None:
                                rejected["account_execution_lock_timeout"] += 1
                            else:
                                before = executor.fills
                                attempts += 1
                                try:
                                    executor.submit_selected_locked(candidate)
                                finally:
                                    executor.release_execution_lock(descriptor)
                                filled = executor.fills > before
                                record_attempt(
                                    ledger,
                                    row,
                                    "filled" if filled else "not_filled",
                                    filled,
                                    allow_repeat=args.allow_repeat_episode_pair,
                                )
            except Exception as exc:
                errors += 1
                status = "cycle_error"
                lab.log_line(args.log, "news_challenger_cycle_error", error=f"{type(exc).__name__}: {exc}"[:500])
            if time.monotonic() >= next_heartbeat:
                write_state(
                    args.heartbeat,
                    status=status,
                    account_id=account_id,
                    accepted=accepted,
                    rejected=rejected,
                    executor=executor,
                    connection=ledger,
                    attempts=attempts,
                    technical_exits=technical_exits,
                    errors=errors,
                    args=args,
                    account_summary=summary,
                    open_trades=trades,
                )
                next_heartbeat = time.monotonic() + args.heartbeat_sec
            time.sleep(args.poll_sec)
    finally:
        write_state(
            args.state,
            status="stopped",
            account_id=account_id,
            accepted=accepted,
            rejected=rejected,
            executor=executor,
            connection=ledger,
            attempts=attempts,
            technical_exits=technical_exits,
            errors=errors,
            args=args,
            account_summary=summary,
            open_trades=trades,
        )
        ledger.close()
        executor.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
