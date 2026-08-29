#!/usr/bin/env python3
"""Read-only practice-007 re-entry cooldown counterfactual ledger."""

from __future__ import annotations

import argparse
import time
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import oanda_practice_shadow_strategy_lab as lab
except ModuleNotFoundError:  # Package imports used by the test suite.
    from trad import oanda_practice_shadow_strategy_lab as lab


CAN_PLACE_ORDERS = False
RESEARCH_ONLY = True
DEFAULT_OUTPUT = (
    Path(__file__).resolve().parent
    / "data"
    / "oanda_training_manager"
    / "state"
    / "practice_007_reentry_cooldown_shadow_v1.json"
)


def epoch(value: Any) -> float:
    text = str(value or "").strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return 0.0


def strategy_trade_episodes(
    transactions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    order_meta: dict[str, dict[str, Any]] = {}
    episodes: dict[str, dict[str, Any]] = {}
    for row in sorted(transactions, key=lambda item: int(item.get("id") or 0)):
        transaction_id = str(row.get("id") or "")
        if row.get("type") == "MARKET_ORDER":
            extensions = row.get("clientExtensions") or {}
            if str(extensions.get("tag") or "") != "strategy_lab_top":
                continue
            order_meta[transaction_id] = {
                "instrument": str(row.get("instrument") or ""),
                "requested_units": lab.safe_float(row.get("units")),
                "signal": str(extensions.get("comment") or ""),
                "entry_time": str(row.get("time") or ""),
            }
            continue
        if row.get("type") != "ORDER_FILL":
            continue
        opened = row.get("tradeOpened") or {}
        order_id = str(row.get("orderID") or "")
        meta = order_meta.get(order_id)
        if opened and meta:
            trade_id = str(opened.get("tradeID") or "")
            if trade_id:
                episodes[trade_id] = {
                    "trade_id": trade_id,
                    **meta,
                    "entry_epoch": epoch(row.get("time")),
                    "entry_price": lab.safe_float(row.get("price")),
                    "units": lab.safe_float(opened.get("units")),
                    "closed": False,
                }
        for closed in row.get("tradesClosed") or []:
            trade_id = str(closed.get("tradeID") or "")
            episode = episodes.get(trade_id)
            if episode is None:
                continue
            episode.update(
                {
                    "closed": True,
                    "exit_time": str(row.get("time") or ""),
                    "exit_epoch": epoch(row.get("time")),
                    "exit_price": lab.safe_float(closed.get("price")),
                    "realized_pl": lab.safe_float(closed.get("realizedPL")),
                    "exit_reason": str(row.get("reason") or ""),
                }
            )
    return sorted(episodes.values(), key=lambda item: item["entry_epoch"])


def cooldown_counterfactual(
    transactions: list[dict[str, Any]],
    cooldowns_sec: list[int],
) -> dict[str, Any]:
    episodes = strategy_trade_episodes(transactions)
    prior_closed_by_instrument: dict[str, list[dict[str, Any]]] = {}
    for episode in episodes:
        if episode.get("closed"):
            prior_closed_by_instrument.setdefault(
                episode["instrument"], []
            ).append(episode)

    reentries: list[dict[str, Any]] = []
    for episode in episodes:
        earlier = [
            row
            for row in prior_closed_by_instrument.get(
                episode["instrument"], []
            )
            if row.get("exit_epoch", 0.0) <= episode["entry_epoch"]
            and row["trade_id"] != episode["trade_id"]
        ]
        if not earlier:
            continue
        prior = max(earlier, key=lambda row: row["exit_epoch"])
        gap_sec = max(0.0, episode["entry_epoch"] - prior["exit_epoch"])
        reentries.append(
            {
                "trade_id": episode["trade_id"],
                "instrument": episode["instrument"],
                "entry_time": episode["entry_time"],
                "gap_after_prior_exit_sec": round(gap_sec, 3),
                "same_direction": (
                    episode.get("units", 0.0) * prior.get("units", 0.0) > 0.0
                ),
                "signal": episode.get("signal", ""),
                "closed": bool(episode.get("closed")),
                "realized_pl": (
                    episode.get("realized_pl")
                    if episode.get("closed")
                    else None
                ),
            }
        )

    def direction_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
        resolved = [row for row in rows if row["realized_pl"] is not None]
        actual_pl = sum(float(row["realized_pl"]) for row in resolved)
        return {
            "entries": len(rows),
            "resolved_entries": len(resolved),
            "wins": sum(float(row["realized_pl"]) > 0.0 for row in resolved),
            "losses": sum(float(row["realized_pl"]) < 0.0 for row in resolved),
            "actual_pl": round(actual_pl, 6),
            "avg_actual_pl": round(actual_pl / len(resolved), 6) if resolved else None,
        }

    direction_type_summary = {
        "same_direction": direction_summary(
            [row for row in reentries if row["same_direction"]]
        ),
        "direction_reversal": direction_summary(
            [row for row in reentries if not row["same_direction"]]
        ),
    }

    arms: dict[str, dict[str, Any]] = {}
    for cooldown in sorted({max(0, int(value)) for value in cooldowns_sec}):
        blocked = [
            row
            for row in reentries
            if row["gap_after_prior_exit_sec"] < cooldown
        ]
        resolved = [row for row in blocked if row["realized_pl"] is not None]
        blocked_actual_pl = sum(float(row["realized_pl"]) for row in resolved)
        arms[str(cooldown)] = {
            "cooldown_sec": cooldown,
            "blocked_entries": len(blocked),
            "resolved_blocked_entries": len(resolved),
            "blocked_wins": sum(float(row["realized_pl"]) > 0.0 for row in resolved),
            "blocked_losses": sum(float(row["realized_pl"]) < 0.0 for row in resolved),
            "blocked_actual_pl": round(blocked_actual_pl, 6),
            "counterfactual_pl_delta_if_blocked": round(-blocked_actual_pl, 6),
            "by_reentry_type": {
                "same_direction": direction_summary(
                    [row for row in blocked if row["same_direction"]]
                ),
                "direction_reversal": direction_summary(
                    [row for row in blocked if not row["same_direction"]]
                ),
            },
        }
    return {
        "schema_version": 1,
        "generated_utc": lab.utc_now(),
        "research_only": RESEARCH_ONLY,
        "can_place_orders": CAN_PLACE_ORDERS,
        "episode_count": len(episodes),
        "closed_episode_count": sum(bool(row.get("closed")) for row in episodes),
        "reentry_count": len(reentries),
        "reentry_direction_summary": direction_type_summary,
        "cooldown_arms": arms,
        "reentries": reentries,
        "interpretation": (
            "Counterfactual accounting only; no cooldown arm changes execution "
            "eligibility without independent validation and deployment review."
        ),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--creds", type=Path, default=lab.DEFAULT_CREDS)
    parser.add_argument("--account-key", default="OANDA_ACCOUNT_ID_DUM4")
    parser.add_argument("--account-id", default=None)
    parser.add_argument("--since-id", default="1")
    parser.add_argument("--cooldowns-sec", default="60,300,900,1800")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--heartbeat-state",
        type=Path,
        default=DEFAULT_OUTPUT.with_name(
            "practice_007_reentry_shadow_heartbeat_v1.json"
        ),
    )
    parser.add_argument("--duration-sec", type=float, default=0.0)
    parser.add_argument("--poll-sec", type=float, default=60.0)
    return parser.parse_args(argv)


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    token, account_id = lab.read_credentials(
        args.creds,
        args.account_key,
        args.account_id,
    )
    client = lab.MarketDataClient(token, timeout_sec=10.0, retries=2)
    payload = client.get(
        f"/v3/accounts/{account_id}/transactions/sinceid",
        params={"id": str(args.since_id)},
    )
    cooldowns = [
        int(value.strip())
        for value in str(args.cooldowns_sec).split(",")
        if value.strip()
    ]
    result = cooldown_counterfactual(
        [row for row in payload.get("transactions", []) if isinstance(row, dict)],
        cooldowns,
    )
    result["account_suffix"] = account_id[-4:]
    result["last_transaction_id"] = str(payload.get("lastTransactionID") or "")
    lab.atomic_json(args.output, result)
    return result


def retryable_broker_error(exc: BaseException) -> bool:
    """Return whether a read-only broker failure should keep the worker alive.

    The supervisor should not churn a research worker during an OANDA outage.
    Authentication and other client errors still fail loudly so configuration
    problems are not hidden.
    """
    status = getattr(exc, "status", None)
    return status == 429 or (isinstance(status, int) and status >= 500)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    stop_at = time.monotonic() + max(0.0, args.duration_sec)
    heartbeat = lab.WorkerHeartbeat(
        args.heartbeat_state,
        worker="oanda_practice_reentry_shadow",
        role="practice_007_read_only_reentry_research",
    ).start()
    consecutive_broker_errors = 0
    try:
        while True:
            heartbeat.update(
                phase="reading_practice_transactions",
                can_place_orders=CAN_PLACE_ORDERS,
                research_only=RESEARCH_ONLY,
            )
            try:
                result = run_once(args)
            except lab.OandaApiError as exc:
                if not retryable_broker_error(exc):
                    raise
                consecutive_broker_errors += 1
                heartbeat.update(
                    phase="broker_read_degraded",
                    can_place_orders=CAN_PLACE_ORDERS,
                    research_only=RESEARCH_ONLY,
                    broker_http_status=getattr(exc, "status", None),
                    consecutive_broker_errors=consecutive_broker_errors,
                    last_error=str(exc),
                    fail_closed=True,
                )
                if args.duration_sec <= 0.0 or time.monotonic() >= stop_at:
                    return 0
                time.sleep(
                    min(
                        max(5.0, args.poll_sec),
                        max(0.0, stop_at - time.monotonic()),
                    )
                )
                continue
            consecutive_broker_errors = 0
            heartbeat.update(
                phase="observing",
                can_place_orders=CAN_PLACE_ORDERS,
                research_only=RESEARCH_ONLY,
                last_transaction_id=result.get("last_transaction_id"),
                episode_count=result.get("episode_count"),
                reentry_count=result.get("reentry_count"),
                cooldown_arms=result.get("cooldown_arms"),
                output=str(args.output.resolve()),
                # Clear any retryable broker status retained in the merged heartbeat
                # details once a subsequent read succeeds.
                broker_http_status=None,
                consecutive_broker_errors=0,
                last_error="",
                fail_closed=False,
            )
            if args.duration_sec <= 0.0 or time.monotonic() >= stop_at:
                return 0
            time.sleep(
                min(
                    max(1.0, args.poll_sec),
                    max(0.0, stop_at - time.monotonic()),
                )
            )
    finally:
        heartbeat.close()


if __name__ == "__main__":
    raise SystemExit(main())
