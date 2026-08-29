#!/usr/bin/env python3
"""Practice-006 21-currency rank challenger.

This is an isolated practice experiment, not a promotion of governed evidence.
It expresses the strongest and weakest independently ranked currency factors
through listed OANDA pairs, confirms the move on the executable price stream,
and reserves each currency for at most one simultaneous thesis.
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
    import oanda_currency_rank_model as rank_model
    import oanda_practice_shadow_strategy_lab as lab
except ModuleNotFoundError:  # Package imports used by the full test suite.
    from trad import oanda_currency_rank_model as rank_model
    from trad import oanda_practice_shadow_strategy_lab as lab


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "oanda_training_manager"
STATE = DATA / "state"
LOGS = DATA / "logs"
DEFAULT_RANK_SOURCE = DATA / "market_sentiment_ticker" / "LATEST.json"
DEFAULT_STATE = STATE / "practice_006_currency_rank_v1.json"
DEFAULT_HEARTBEAT = STATE / "practice_006_currency_rank_heartbeat_v1.json"
DEFAULT_LEDGER = STATE / "practice_006_currency_rank_v1.sqlite"
DEFAULT_LOG = LOGS / "practice_006_currency_rank_v1.jsonl"
DEFAULT_LOCK = STATE / "practice_006_order.lock"
UTC = timezone.utc


def load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def open_ledger(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=5.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS theses(
            thesis_id TEXT PRIMARY KEY,
            active INTEGER NOT NULL,
            active_since_utc TEXT NOT NULL,
            last_seen_utc TEXT NOT NULL,
            attempted INTEGER NOT NULL,
            attempted_utc TEXT NOT NULL DEFAULT '',
            last_status TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS attempts(
            attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
            thesis_id TEXT NOT NULL,
            attempted_utc TEXT NOT NULL,
            utc_day TEXT NOT NULL,
            instrument TEXT NOT NULL,
            direction TEXT NOT NULL,
            status TEXT NOT NULL,
            fill_count INTEGER NOT NULL,
            payload_json TEXT NOT NULL
        )
        """
    )
    connection.commit()
    return connection


def synchronize_theses(
    connection: sqlite3.Connection,
    selected: list[dict[str, Any]],
) -> None:
    """Reset re-entry only after a rank thesis genuinely disappears."""
    now = datetime.now(UTC).isoformat()
    active_ids = {str(row.get("thesis_id") or "") for row in selected}
    for (thesis_id,) in connection.execute("SELECT thesis_id FROM theses WHERE active=1"):
        if thesis_id not in active_ids:
            connection.execute("UPDATE theses SET active=0 WHERE thesis_id=?", (thesis_id,))
    for row in selected:
        thesis_id = str(row.get("thesis_id") or "")
        existing = connection.execute(
            "SELECT active FROM theses WHERE thesis_id=?", (thesis_id,)
        ).fetchone()
        payload = json.dumps(row, sort_keys=True, default=str)
        if existing is None:
            connection.execute(
                "INSERT INTO theses VALUES (?,?,?,?,?,?,?,?)",
                (thesis_id, 1, now, now, 0, "", "", payload),
            )
        elif int(existing[0]) == 0:
            connection.execute(
                "UPDATE theses SET active=1,active_since_utc=?,last_seen_utc=?,"
                "attempted=0,attempted_utc='',last_status='',payload_json=? WHERE thesis_id=?",
                (now, now, payload, thesis_id),
            )
        else:
            connection.execute(
                "UPDATE theses SET last_seen_utc=?,payload_json=? WHERE thesis_id=?",
                (now, payload, thesis_id),
            )
    connection.commit()


def thesis_attempted(connection: sqlite3.Connection, thesis_id: str) -> bool:
    row = connection.execute(
        "SELECT attempted FROM theses WHERE thesis_id=? AND active=1", (thesis_id,)
    ).fetchone()
    return row is not None and int(row[0]) == 1


def record_attempt(
    connection: sqlite3.Connection,
    row: Mapping[str, Any],
    *,
    status: str,
    filled: bool,
) -> None:
    now = datetime.now(UTC)
    thesis_id = str(row.get("thesis_id") or "")
    payload = json.dumps(dict(row), sort_keys=True, default=str)
    connection.execute(
        "INSERT INTO attempts(thesis_id,attempted_utc,utc_day,instrument,direction,status,fill_count,payload_json) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (
            thesis_id, now.isoformat(), now.date().isoformat(),
            str(row.get("instrument") or ""), str(row.get("direction") or ""),
            status, int(filled), payload,
        ),
    )
    connection.execute(
        "UPDATE theses SET attempted=1,attempted_utc=?,last_status=? WHERE thesis_id=?",
        (now.isoformat(), status, thesis_id),
    )
    connection.commit()


def daily_fills(connection: sqlite3.Connection) -> int:
    day = datetime.now(UTC).date().isoformat()
    row = connection.execute(
        "SELECT COALESCE(SUM(fill_count),0) FROM attempts WHERE utc_day=?", (day,)
    ).fetchone()
    return int(row[0] if row else 0)


def reserved_currencies(trades: list[dict[str, Any]]) -> set[str]:
    result: set[str] = set()
    for trade in trades:
        pair = str(trade.get("instrument") or "").split("_")
        if len(pair) == 2:
            result.update(pair)
    return result


class CurrencyRankPracticeExecutor(lab.PracticeExecutor):
    def owns_trade(self, trade: dict[str, Any]) -> bool:
        extensions = trade.get("clientExtensions") or {}
        return (
            str(extensions.get("tag") or "") == "strategy_lab_top"
            and str(extensions.get("comment") or "").startswith("rank006.")
        )

    def final_submission_blocker(
        self,
        candidate: dict[str, Any],
        units: int,
        order_body: dict[str, Any],
    ) -> str:
        if "practice" not in str(lab.BASE_URL).lower():
            return "currency_rank_not_practice_endpoint"
        if not self.account_id.endswith("-006"):
            return "currency_rank_not_account_006"
        if units != 100:
            return "currency_rank_units_not_fixed_100"
        if str(candidate.get("model_id") or "") != rank_model.MODEL_ID:
            return "currency_rank_model_identity_invalid"
        if not str(candidate.get("lane_id") or "").startswith("rank006."):
            return "currency_rank_lane_identity_invalid"
        if candidate.get("rank_source_ready") is not True:
            return "currency_rank_source_not_ready"
        if int(lab.safe_float(candidate.get("rank_currency_count"))) != 21:
            return "currency_rank_currency_universe_invalid"
        if int(lab.safe_float(candidate.get("rank_instrument_count"))) != 68:
            return "currency_rank_instrument_universe_invalid"
        instrument = str(candidate.get("instrument") or "")
        strong = str(candidate.get("strong_currency") or "")
        weak = str(candidate.get("weak_currency") or "")
        if strong == weak or strong not in rank_model.EXPECTED_CURRENCIES or weak not in rank_model.EXPECTED_CURRENCIES:
            return "currency_rank_factor_identity_invalid"
        base, quote = instrument.split("_", 1) if "_" in instrument else ("", "")
        expected = "buy" if (base, quote) == (strong, weak) else "sell" if (base, quote) == (weak, strong) else ""
        if expected != str(candidate.get("direction") or ""):
            return "currency_rank_pair_direction_invalid"
        return ""


def configure_executor_args(args: argparse.Namespace) -> argparse.Namespace:
    result = lab.parse_args([])
    result.execution_signal_feed_database = args.signal_feed_database
    result.execution_policy_state = None
    result.execution_lock_path = args.execution_lock
    result.execution_feed_source = "practice_006_currency_rank"
    result.execution_signal_snapshot = None
    result.execution_cooldown_sec = args.execution_cooldown_sec
    result.execution_max_hold_sec = float(args.max_hold_sec)
    result.execution_max_quote_age_sec = args.max_quote_age_sec
    result.execution_max_slippage_pips = args.max_slippage_pips
    result.execution_max_loss_account = args.max_loss_account
    result.execution_min_signal_confidence = 0.50
    result.execution_min_signal_expected_net_pips = args.min_after_cost_pips
    result.execution_allow_unvalidated_signals = True
    result.execution_prediction_quality = False
    result.execution_prediction_quality_negative_veto = False
    result.execution_second_curve_entry_veto = False
    result.execution_second_curve_profit_exit = False
    result.execution_dynamic_sizing = False
    result.execution_units = 100
    result.execution_max_units = 100
    result.execution_max_open_positions = args.max_open_positions
    result.execution_max_currency_direction_positions = 1
    result.execution_max_currency_direction_margin_pct = 20.0
    result.execution_target_margin_used_pct = 20.0
    result.execution_high_confidence_margin_used_pct = 25.0
    result.execution_hard_margin_used_pct = 35.0
    result.execution_min_trade_margin_pct = 0.01
    result.execution_max_trade_margin_pct = 1.0
    result.execution_min_risk_pct = 0.01
    result.execution_max_risk_pct = 0.10
    result.execution_reentry_cooldown_sec = 0.0
    result.execution_instrument_reentry_cooldown_sec = 0.0
    result.execution_jpy_factor_cooldown_sec = 0.0
    result.execution_max_open_jpy_factor_positions = 1
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
    result.execution_horizons = [300, 900, 3600]
    return result


def candidate_from_rank(
    row: Mapping[str, Any],
    quote: Any,
    instrument_meta: Mapping[str, Any],
    rank_result: Mapping[str, Any],
    args: argparse.Namespace,
) -> tuple[dict[str, Any] | None, str]:
    bid = lab.safe_float(getattr(quote, "bid", 0.0))
    ask = lab.safe_float(getattr(quote, "ask", 0.0))
    if bid <= 0.0 or ask <= bid:
        return None, "live_quote_invalid"
    pip = 10.0 ** int(lab.safe_float(instrument_meta.get("pip_location"), -4.0))
    spread_pips = (ask - bid) / max(pip, 1e-12)
    if spread_pips > args.max_spread_pips:
        return None, "live_spread_above_cap"
    mid = (bid + ask) / 2.0
    gross_pips = lab.safe_float(row.get("gross_movement_proxy_bps")) * mid * 0.0001 / max(pip, 1e-12)
    after_cost = gross_pips - args.live_spread_stress_multiple * spread_pips
    ratio = gross_pips / max(args.live_spread_stress_multiple * spread_pips, 1e-9)
    if ratio < args.min_gross_to_spread or after_cost < args.min_after_cost_pips:
        return None, "live_after_cost_gate_failed"
    score_gap = max(0.0, lab.safe_float(row.get("strong_score")) - lab.safe_float(row.get("weak_score")))
    confidence = max(0.50, min(0.72, 0.50 + 0.035 * score_gap + 0.01 * min(5.0, ratio)))
    stop = min(
        args.max_stop_pips,
        max(args.min_stop_pips, spread_pips * args.stop_spread_multiple, 0.75 * gross_pips),
    )
    strong = str(row.get("strong_currency") or "")
    weak = str(row.get("weak_currency") or "")
    return {
        "id": str(row.get("thesis_id") or ""),
        "lane_id": f"rank006.{strong}-{weak}",
        "family": "cross_currency_rank_rotation",
        "profile": "practice_006_currency_rank_challenger",
        "model_id": rank_model.MODEL_ID,
        "instrument": str(row.get("instrument") or ""),
        "direction": str(row.get("direction") or ""),
        "bid": bid,
        "ask": ask,
        "pip": pip,
        "entry_time": str(getattr(quote, "time", "") or lab.utc_now()),
        "forecast_created_monotonic": time.monotonic(),
        "execution_horizon_sec": args.max_hold_sec,
        "execution_exit_horizon_sec": args.max_hold_sec,
        "stop_loss_pips": round(stop, 3),
        "take_profit_r": args.take_profit_r,
        "open_ended_profit": True,
        "spread_pips": round(spread_pips, 4),
        "signal_strength_pips": round(gross_pips, 4),
        "signal_to_spread": round(gross_pips / max(spread_pips, 1e-9), 6),
        "gross_to_spread": round(ratio, 6),
        "projected_gross_movement_pips": round(gross_pips, 4),
        "projected_net_pips": round(after_cost, 4),
        "instant_projected_net_pips": round(after_cost, 4),
        "liquidity_quality": 1.0,
        "signal_confidence": confidence,
        "signal_score": lab.safe_float(row.get("rank_score")),
        "strong_currency": strong,
        "weak_currency": weak,
        "rank_thesis_id": str(row.get("thesis_id") or ""),
        "rank_source_ready": str(rank_result.get("status") or "") == "ready",
        "rank_currency_count": int(lab.safe_float(rank_result.get("currency_count"))),
        "rank_instrument_count": int(lab.safe_float(rank_result.get("instrument_count"))),
        "rank_snapshot_utc": str(rank_result.get("generated_utc") or ""),
        "research_only_challenger": True,
    }, ""


def rank_thesis_state(
    rank_result: Mapping[str, Any],
    strong: str,
    weak: str,
) -> tuple[str, dict[str, float]]:
    ranks = {
        str(row.get("currency") or ""): row
        for row in rank_result.get("currency_ranks") or []
        if isinstance(row, Mapping)
    }
    if strong not in ranks or weak not in ranks:
        return "unknown", {}
    strong_row, weak_row = ranks[strong], ranks[weak]
    detail = {
        "score_gap": lab.safe_float(strong_row.get("score")) - lab.safe_float(weak_row.get("score")),
        "factor_gap_5m_bps": lab.safe_float(strong_row.get("strength_5m_bps")) - lab.safe_float(weak_row.get("strength_5m_bps")),
        "factor_gap_15m_bps": lab.safe_float(strong_row.get("strength_15m_bps")) - lab.safe_float(weak_row.get("strength_15m_bps")),
    }
    valid = detail["score_gap"] > 0.0 and (
        detail["factor_gap_5m_bps"] > 0.0 or detail["factor_gap_15m_bps"] > 0.0
    )
    return ("valid" if valid else "reversed"), detail


def close_rank_invalidations(
    executor: CurrencyRankPracticeExecutor,
    rank_result: Mapping[str, Any],
    args: argparse.Namespace,
) -> int:
    closed = 0
    now = datetime.now(UTC)
    for trade in executor.open_trades():
        if not executor.owns_trade(trade):
            continue
        opened = lab.parse_rfc3339(str(trade.get("openTime") or ""))
        if opened is None or (now - opened).total_seconds() < args.min_rank_exit_age_sec:
            continue
        side = "buy" if lab.safe_float(trade.get("currentUnits")) > 0.0 else "sell"
        instrument = str(trade.get("instrument") or "")
        comment = str((trade.get("clientExtensions") or {}).get("comment") or "")
        identity = comment.split("|", 1)[0]
        factor_text = identity[len("rank006."):] if identity.startswith("rank006.") else ""
        factor_pair = factor_text.split("-", 1)
        if len(factor_pair) != 2:
            # Missing/malformed factor identity is an observability problem,
            # not evidence that the market thesis reversed.
            continue
        strong, weak = factor_pair
        thesis_state, detail = rank_thesis_state(rank_result, strong, weak)
        if thesis_state != "reversed":
            continue
        descriptor = executor.acquire_execution_lock(timeout_sec=0.5)
        if descriptor is None:
            continue
        try:
            response = executor.client.write(
                "PUT", f"/v3/accounts/{executor.account_id}/trades/{trade.get('id')}/close", {"units": "ALL"}
            )
            fill = response.get("orderFillTransaction") or {}
            lab.log_line(
                executor.log_path, "currency_rank_invalidation_exit",
                trade_id=str(trade.get("id") or ""), instrument=instrument,
                direction=side, strong_currency=strong, weak_currency=weak,
                score_gap=round(detail["score_gap"], 6),
                factor_gap_5m_bps=round(detail["factor_gap_5m_bps"], 6),
                factor_gap_15m_bps=round(detail["factor_gap_15m_bps"], 6),
                realized_pl=lab.safe_float(fill.get("pl")),
                request_id=response.get("_requestID"),
            )
            closed += 1
        except lab.OandaApiError as exc:
            lab.log_line(
                executor.log_path, "currency_rank_invalidation_exit_error",
                trade_id=str(trade.get("id") or ""), instrument=instrument,
                outcome_uncertain=exc.outcome_uncertain, error=str(exc)[:500],
            )
        finally:
            executor.release_execution_lock(descriptor)
    return closed


def write_state(
    path: Path,
    *,
    status: str,
    account_id: str,
    rank_result: Mapping[str, Any],
    executor: CurrencyRankPracticeExecutor,
    connection: sqlite3.Connection,
    attempts: int,
    rank_exits: int,
    errors: int,
    args: argparse.Namespace,
    account_summary: Mapping[str, Any],
    open_trades: list[dict[str, Any]],
) -> None:
    lab.atomic_json(
        path,
        {
            "schema_version": "practice_006_currency_rank_v1",
            "updated_at": lab.utc_now(),
            "status": status,
            "pid": os.getpid(),
            "environment": "practice",
            "account_suffix": account_id[-4:],
            "model_id": rank_model.MODEL_ID,
            "account": {
                "balance": lab.safe_float(account_summary.get("balance")),
                "nav": lab.safe_float(account_summary.get("NAV")),
                "cumulative_pl": lab.safe_float(account_summary.get("pl")),
                "unrealized_pl": lab.safe_float(account_summary.get("unrealizedPL")),
                "margin_used": lab.safe_float(account_summary.get("marginUsed")),
                "margin_available": lab.safe_float(account_summary.get("marginAvailable")),
                "open_trade_count": len(open_trades),
                "owned_open_trade_count": sum(1 for trade in open_trades if executor.owns_trade(trade)),
                "pending_order_count": int(lab.safe_float(account_summary.get("pendingOrderCount"))),
                "positions": [
                    {
                        "trade_id": str(trade.get("id") or ""),
                        "instrument": str(trade.get("instrument") or ""),
                        "direction": "long" if lab.safe_float(trade.get("currentUnits")) > 0.0 else "short",
                        "units": abs(lab.safe_float(trade.get("currentUnits"))),
                        "entry_price": lab.safe_float(trade.get("price")),
                        "unrealized_pl": lab.safe_float(trade.get("unrealizedPL")),
                        "owned": executor.owns_trade(trade),
                    }
                    for trade in open_trades
                ],
            },
            "policy": {
                "research_only_challenger": True,
                "real_money_route": False,
                "direction_source": "ranked_21_currency_factors",
                "entry_timing": "aligned_5m_and_15m_pair_reaction_after_stressed_cost",
                "exit_timing": "rank_invalidation_or_broker_protection_or_h1",
                "factor_deduplication": "one_open_expression_per_currency",
                "reentry_rule": "one_attempt_per_continuous_rank_thesis_reset_only_after_disappearance",
                "fixed_units": 100,
                "max_total_open_positions": args.max_open_positions,
                "max_daily_fills": args.max_daily_fills,
                "core_governed_executor_unchanged": True,
            },
            "rank_snapshot_utc": str(rank_result.get("generated_utc") or ""),
            "rank_snapshot_age_sec": rank_result.get("snapshot_age_sec"),
            "currency_count": int(lab.safe_float(rank_result.get("currency_count"))),
            "instrument_count": int(lab.safe_float(rank_result.get("instrument_count"))),
            "currency_ranks": list(rank_result.get("currency_ranks") or []),
            "candidate_count": len(rank_result.get("pair_candidates") or []),
            "selected_count": len(rank_result.get("selected_pairs") or []),
            "selected_pairs": list(rank_result.get("selected_pairs") or [])[:8],
            "rejection_counts": dict(rank_result.get("rejection_counts") or {}),
            "attempts_since_start": attempts,
            "fills_since_start": executor.fills,
            "fills_today_utc": daily_fills(connection),
            "rank_exits_since_start": rank_exits,
            "errors_since_start": errors,
            "disabled_reason": executor.disabled_reason,
        },
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--creds", type=Path, default=lab.DEFAULT_CREDS)
    parser.add_argument("--account-key", default="OANDA_ACCOUNT_ID_DUM3")
    parser.add_argument("--account-id", default="")
    parser.add_argument("--rank-source", type=Path, default=DEFAULT_RANK_SOURCE)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--heartbeat", type=Path, default=DEFAULT_HEARTBEAT)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG)
    parser.add_argument("--signal-feed-database", type=Path, default=STATE / "practice_006_currency_rank_signal_feed_v1.sqlite")
    parser.add_argument("--execution-lock", type=Path, default=DEFAULT_LOCK)
    parser.add_argument("--duration-sec", type=float, default=604800.0)
    parser.add_argument("--poll-sec", type=float, default=1.0)
    parser.add_argument("--heartbeat-sec", type=float, default=2.0)
    parser.add_argument("--max-snapshot-age-sec", type=float, default=90.0)
    parser.add_argument("--min-leg-strength-bps", type=float, default=0.10)
    parser.add_argument("--min-factor-gap-bps", type=float, default=0.50)
    parser.add_argument("--min-pair-confirmation-bps", type=float, default=0.20)
    parser.add_argument("--min-confirmation-to-cost", type=float, default=1.05)
    parser.add_argument("--rank-spread-stress-multiple", type=float, default=1.20)
    parser.add_argument("--max-spread-bps", type=float, default=12.0)
    parser.add_argument("--max-spread-pips", type=float, default=30.0)
    parser.add_argument("--live-spread-stress-multiple", type=float, default=1.20)
    parser.add_argument("--min-gross-to-spread", type=float, default=1.05)
    parser.add_argument("--min-after-cost-pips", type=float, default=0.05)
    parser.add_argument("--max-hold-sec", type=int, default=3600)
    parser.add_argument("--min-rank-exit-age-sec", type=float, default=60.0)
    parser.add_argument("--max-open-positions", type=int, default=8)
    parser.add_argument("--max-daily-fills", type=int, default=48)
    parser.add_argument("--execution-cooldown-sec", type=float, default=2.0)
    parser.add_argument("--max-quote-age-sec", type=float, default=4.0)
    parser.add_argument("--max-slippage-pips", type=float, default=1.0)
    parser.add_argument("--max-loss-account", type=float, default=1.0)
    parser.add_argument("--min-stop-pips", type=float, default=4.0)
    parser.add_argument("--max-stop-pips", type=float, default=30.0)
    parser.add_argument("--stop-spread-multiple", type=float, default=3.0)
    parser.add_argument("--take-profit-r", type=float, default=1.5)
    parser.add_argument("--manage-interval-sec", type=float, default=0.25)
    parser.add_argument("--min-trailing-pips", type=float, default=1.5)
    parser.add_argument("--trailing-spread-multiple", type=float, default=1.75)
    parser.add_argument("--trailing-stop-r", type=float, default=0.45)
    parser.add_argument("--trailing-activation-r", type=float, default=0.70)
    parser.add_argument("--profit-lock-trigger-pips", type=float, default=1.5)
    args = parser.parse_args(argv)
    if not 1 <= args.max_open_positions <= 8 or args.max_daily_fills < 1 or args.poll_sec <= 0:
        parser.error("one-to-eight positions, daily fills, and polling must be positive")
    return args


def run(args: argparse.Namespace) -> int:
    if "practice" not in str(lab.BASE_URL).lower():
        raise SystemExit("Currency-rank challenger is restricted to OANDA practice.")
    token, account_id = lab.read_credentials(args.creds, args.account_key, args.account_id)
    if not account_id.endswith("-006"):
        raise SystemExit("Currency-rank challenger is restricted to account suffix -006.")
    executor = CurrencyRankPracticeExecutor(
        lab.MarketDataClient(token), account_id, args.log, lab.LanePerformance(60),
        configure_executor_args(args), exit_fit=None, promotion=None,
    )
    executor.initialize()
    ledger = open_ledger(args.ledger)
    attempts = rank_exits = errors = 0
    rank_result: dict[str, Any] = {}
    trades: list[dict[str, Any]] = []
    summary: dict[str, Any] = {}
    next_heartbeat = 0.0
    started = time.monotonic()
    lab.log_line(
        args.log, "currency_rank_challenger_started", account_suffix=account_id[-4:],
        model_id=rank_model.MODEL_ID, currency_count=21, instrument_count=68,
        fixed_units=100, max_open_positions=args.max_open_positions,
        real_money_route=False,
    )
    settings = rank_model.RankSettings(
        max_snapshot_age_sec=args.max_snapshot_age_sec,
        min_leg_strength_bps=args.min_leg_strength_bps,
        min_factor_gap_bps=args.min_factor_gap_bps,
        min_pair_confirmation_bps=args.min_pair_confirmation_bps,
        min_confirmation_to_cost=args.min_confirmation_to_cost,
        spread_stress_multiple=args.rank_spread_stress_multiple,
        max_spread_bps=args.max_spread_bps,
        max_selected=args.max_open_positions,
    )
    try:
        while time.monotonic() - started < args.duration_sec:
            status = "running"
            try:
                executor.manage_open_trades()
                trades = executor.open_trades()
                rank_result = rank_model.rank_snapshot(
                    load_json(args.rank_source), settings=settings,
                    reserved_currencies=reserved_currencies(trades),
                )
                status = "running" if rank_result.get("status") == "ready" else "rank_source_blocked"
                selected = list(rank_result.get("selected_pairs") or [])
                continuing_hypotheses = list(rank_result.get("rank_theses") or [])
                # An owned trade reserves its two currencies and is therefore
                # intentionally absent from selected_pairs.  Keep its thesis
                # active, and its rank-validity check intact, from the full
                # cost-clearing candidate set so an open position cannot reset
                # its own re-entry generation or close itself spuriously.
                if rank_result.get("status") == "ready":
                    synchronize_theses(ledger, continuing_hypotheses)
                    rank_exits += close_rank_invalidations(executor, rank_result, args)
                trades = executor.open_trades()
                summary = executor.client.get(f"/v3/accounts/{account_id}/summary", params={}).get("account") or {}
                if executor.start_balance - lab.safe_float(summary.get("balance")) >= args.max_loss_account:
                    executor.disabled_reason = "currency_rank_session_loss_limit"
                    status = "session_loss_limit"
                elif daily_fills(ledger) >= args.max_daily_fills:
                    status = "daily_fill_cap"
                elif len(trades) < args.max_open_positions:
                    open_instruments = {str(trade.get("instrument") or "") for trade in trades}
                    selectable = [
                        row for row in selected
                        if str(row.get("instrument") or "") not in open_instruments
                        and not thesis_attempted(ledger, str(row.get("thesis_id") or ""))
                    ]
                    if selectable:
                        row = selectable[0]
                        instrument = str(row.get("instrument") or "")
                        quote = executor.client.pricing_snapshot(account_id, [instrument]).get(instrument)
                        candidate, reason = (
                            candidate_from_rank(
                                row, quote, executor.instrument_meta.get(instrument) or {}, rank_result, args
                            ) if quote is not None else (None, "live_quote_missing")
                        )
                        if candidate is None:
                            rank_result.setdefault("rejection_counts", {})[reason] = (
                                int(rank_result.get("rejection_counts", {}).get(reason, 0)) + 1
                            )
                        else:
                            descriptor = executor.acquire_execution_lock(timeout_sec=0.5)
                            if descriptor is None:
                                rank_result.setdefault("rejection_counts", {})["account_execution_lock_timeout"] = 1
                            else:
                                before = executor.fills
                                attempts += 1
                                try:
                                    executor.submit_selected_locked(candidate)
                                finally:
                                    executor.release_execution_lock(descriptor)
                                filled = executor.fills > before
                                record_attempt(
                                    ledger, row, status="filled" if filled else "not_filled", filled=filled
                                )
            except Exception as exc:
                errors += 1
                status = "cycle_error"
                lab.log_line(args.log, "currency_rank_cycle_error", error=f"{type(exc).__name__}: {exc}"[:500])
            if time.monotonic() >= next_heartbeat:
                write_state(
                    args.heartbeat, status=status, account_id=account_id,
                    rank_result=rank_result, executor=executor, connection=ledger,
                    attempts=attempts, rank_exits=rank_exits, errors=errors, args=args,
                    account_summary=summary, open_trades=trades,
                )
                next_heartbeat = time.monotonic() + args.heartbeat_sec
            time.sleep(args.poll_sec)
    finally:
        write_state(
            args.state, status="stopped", account_id=account_id,
            rank_result=rank_result, executor=executor, connection=ledger,
            attempts=attempts, rank_exits=rank_exits, errors=errors, args=args,
            account_summary=summary, open_trades=trades,
        )
        ledger.close()
        executor.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
